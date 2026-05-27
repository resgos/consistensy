package ru.sbrf.pprb.stmnt.consistency.rpc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.springframework.web.bind.annotation.*;
import ru.sbrf.pprb.stmnt.consistency.config.ConsistencyProperties;
import ru.sbrf.pprb.stmnt.consistency.integration.ignite.IgniteClientFactory;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Мини-нагрузочный тест: запускает SQL N раз, замеряет latency
 * (avg, p50, p95, p99, max) — для сравнения планов запросов на разных
 * кластерах и оценки реального impact'а INDEX vs FULL_SCAN.
 *
 *   POST /api/debug/perf
 *   { "name": "SQL_SUM_BETWEEN",
 *     "sql": "SELECT SUM(CCSUM) FROM TURNDOCCUR WHERE REGISTER=? AND CCOPERATIONDAY>=?",
 *     "args": ["R001", "2026-05-01"],
 *     "iterations": 100,
 *     "clusters": ["cluster-1","cluster-2"]   // optional, default all
 *   }
 *
 * Замеры: первая итерация (cold) выкидывается из stats (JIT warmup).
 * Возвращает рекорд latency per-cluster в миллисекундах.
 *
 * Также: POST /api/debug/perf-suite — прогон фиксированного набора
 * "реальных" запросов из проекта на всех кластерах разом для сравнения.
 */
@Slf4j
@RestController
@RequestMapping("/api/debug")
@RequiredArgsConstructor
public class PerfTestController {

    private final ConsistencyProperties props;
    private final IgniteClientFactory clientFactory;

    @PostMapping("/perf")
    public Map<String, Object> perf(@RequestBody PerfRequest req) {
        List<String> targets = (req.clusters == null || req.clusters.isEmpty())
                ? props.getClusters().stream().map(ConsistencyProperties.Cluster::getId).toList()
                : req.clusters;
        int iters = req.iterations == null ? 100 : req.iterations;
        Object[] args = req.args == null ? new Object[0] : req.args.toArray();

        Map<String, Object> per = new LinkedHashMap<>();
        for (String cid : targets) {
            IgniteClient c = clientFactory.get(cid);
            if (c == null) { per.put(cid, Map.of("ok", false, "error", "not connected")); continue; }
            try {
                per.put(cid, runOne(c, req.sql, args, iters));
            } catch (Exception e) {
                per.put(cid, Map.of("ok", false, "error", e.toString()));
            }
        }
        return Map.of(
                "name", req.name == null ? "perf" : req.name,
                "sql", req.sql,
                "args", req.args,
                "iterations", iters,
                "perCluster", per);
    }

    @PostMapping("/perf-suite")
    public Map<String, Object> perfSuite(@RequestParam(defaultValue = "100") int iterations,
                                          @RequestParam(defaultValue = "R001") String register) {
        Map<String, PerfRequest> suite = new LinkedHashMap<>();

        suite.put("hasher.REGISTER", req(iterations,
                "SELECT OBJECTID, CCRQTM, CCOPENDATE, CCCLOSEDATE, CURRENCY, CCBALANCERECALCDATE FROM REGISTER"));

        suite.put("hasher.TURN_DOC_CUR.range_3d", req(iterations,
                "SELECT OBJECTID, CCTYPEOPER, CCSUM FROM TURNDOCCUR WHERE CCOPERATIONDAY >= DATE '2026-05-22'"));

        suite.put("hasher.DAY_BALANCES.range_3d", req(iterations,
                "SELECT REGISTER, CCOPERATIONDAY, CCSTARTSUM FROM DAYBALANCES WHERE CCOPERATIONDAY >= DATE '2026-05-22'"));

        // Запрос с WHERE REGISTER + range — это hot-path для DayBalancesRecalcService
        suite.put("SQL_SUM_BETWEEN.byRegister", req(iterations,
                "SELECT COALESCE(SUM(CASE WHEN CCDT='1' THEN -1*CCSUM ELSE CCSUM END), 0) " +
                "FROM TURNDOCCUR WHERE REGISTER=? AND CCTYPEOPER IN (0,40) " +
                "AND CCOPERATIONDAY>=DATE '2026-05-22' AND CCOPERATIONDAY<DATE '2026-05-25'",
                register));

        suite.put("SQL_DAY_AGGREGATES.byRegister", req(iterations,
                "SELECT CCOPERATIONDAY, CCDT, SUM(CCSUM), COUNT(*) " +
                "FROM TURNDOCCUR " +
                "WHERE REGISTER=? AND CCTYPEOPER IN (0,40) " +
                "AND CCOPERATIONDAY>=DATE '2026-05-01' AND CCOPERATIONDAY<=DATE '2026-05-24' " +
                "GROUP BY CCOPERATIONDAY, CCDT",
                register));

        suite.put("REGISTER.byId (point lookup)", req(iterations,
                "SELECT * FROM REGISTER WHERE OBJECTID=?", register));

        suite.put("DAYBALANCES.byRegisterDate (point lookup)", req(iterations,
                "SELECT * FROM DAYBALANCES WHERE REGISTER=? AND CCOPERATIONDAY=DATE '2026-05-24'",
                register));

        Map<String, Object> out = new LinkedHashMap<>();
        for (var e : suite.entrySet()) {
            PerfRequest r = e.getValue();
            r.name = e.getKey();
            out.put(e.getKey(), perf(r));
        }
        return out;
    }

    private static PerfRequest req(int iter, String sql, Object... args) {
        PerfRequest r = new PerfRequest();
        r.iterations = iter;
        r.sql = sql;
        r.args = args == null ? List.of() : Arrays.asList(args);
        return r;
    }

    private Map<String, Object> runOne(IgniteClient c, String sql, Object[] args, int iters) {
        // Warmup
        try (var cur = c.query(new SqlFieldsQuery(sql).setArgs(args))) {
            cur.iterator().forEachRemaining(r -> {});
        }
        // Measure
        long[] timings = new long[iters];
        int rowsLast = 0;
        for (int i = 0; i < iters; i++) {
            long t0 = System.nanoTime();
            int rows = 0;
            try (var cur = c.query(new SqlFieldsQuery(sql).setArgs(args))) {
                for (var r : cur) rows++;
            }
            timings[i] = System.nanoTime() - t0;
            rowsLast = rows;
        }
        Arrays.sort(timings);
        long sum = 0; for (long t : timings) sum += t;
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("rows", rowsLast);
        stats.put("iterations", iters);
        stats.put("avgMs",  Math.round(sum / (double) iters / 1_000_000.0 * 100.0) / 100.0);
        stats.put("p50Ms",  msAt(timings, 0.50));
        stats.put("p95Ms",  msAt(timings, 0.95));
        stats.put("p99Ms",  msAt(timings, 0.99));
        stats.put("maxMs",  Math.round(timings[iters - 1] / 1_000_000.0 * 100.0) / 100.0);
        return stats;
    }

    private static double msAt(long[] sorted, double q) {
        int idx = Math.min(sorted.length - 1, (int) Math.ceil(q * sorted.length) - 1);
        return Math.round(sorted[Math.max(0, idx)] / 1_000_000.0 * 100.0) / 100.0;
    }

    public static class PerfRequest {
        public String name;
        public String sql;
        public List<Object> args;
        public Integer iterations;
        public List<String> clusters;
    }
}
