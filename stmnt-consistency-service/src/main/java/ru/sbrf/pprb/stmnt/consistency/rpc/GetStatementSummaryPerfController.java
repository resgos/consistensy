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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * НТ для getStatementSummary: воспроизводит реальный flow одного request'а
 * как набор SQL-запросов, выполняет N requests с concurrency=K — даёт
 * адекватный нагрузочный профиль end-to-end.
 *
 * Один summary-request делает несколько SQL:
 *   1. SELECT REGISTER WHERE OBJECTID=?              point lookup (~1 ms)
 *   2. SELECT DAYBALANCES WHERE REGISTER=? AND day BETWEEN ?..?
 *                                                    range scan через PK
 *   3. SELECT TURNDOCCUR с aggregates (SUM/CASE WHEN)
 *      WHERE REGISTER=? AND CCTYPEOPER IN (0,40) AND ccOperationDay BETWEEN ?..?
 *                                                    index seek (~3 ms)
 *   4. SELECT TURNDOCCUR с type-50 фильтром
 *      WHERE REGISTER=? AND CCTYPEOPER=50 AND CCOPERATIONDAY BETWEEN ?..?
 *                                                    index seek (~2 ms)
 *   5. SELECT TURNDOCCUR LIMIT 100 ORDER BY CCOPERATIONDAY DESC
 *      WHERE REGISTER=? AND CCOPERATIONDAY BETWEEN ?..?  (paginated list)
 *                                                    range + sort
 *
 * Endpoints:
 *   POST /api/perf/summary?iterations=500&concurrency=10&clusterId=cluster-1
 *
 * Возвращает aggregated metrics: throughput, p50/p95/p99/max latency,
 * ошибки.
 */
@Slf4j
@RestController
@RequestMapping("/api/perf")
@RequiredArgsConstructor
public class GetStatementSummaryPerfController {

    private final ConsistencyProperties props;
    private final IgniteClientFactory clientFactory;

    private static final LocalDate TODAY = LocalDate.of(2026, 5, 24);
    private static final int DAYS_BACK = 14;
    private static final int REGISTER_COUNT = 500;

    /**
     * НТ роста: N регистров на один summary-request. Сравнение двух стратегий:
     *   (a) MULTI-IN: один SQL с WHERE REGISTER IN (?,?,...?)
     *   (b) SPLIT:    N independent SQL по одному регистру каждый
     *
     *  POST /api/perf/summary-multi-reg?registersPerRequest=10&iterations=50
     *
     * Каждая итерация = 1 summary-request, который агрегирует данные по
     * N=registersPerRequest регистрам. Замер per-strategy с p50/p95/p99.
     */
    @PostMapping("/summary-multi-reg")
    public Map<String, Object> summaryMultiReg(
            @RequestParam(defaultValue = "10")  int registersPerRequest,
            @RequestParam(defaultValue = "50")  int iterations,
            @RequestParam(defaultValue = "cluster-1") String clusterId) {

        IgniteClient c = clientFactory.get(clusterId);
        if (c == null) return Map.of("ok", false, "error", "cluster not connected: " + clusterId);

        // Случайные N register-id для каждой итерации (стабильные между запусками для warmup)
        List<List<String>> sets = new ArrayList<>();
        for (int i = 0; i < iterations + 1; i++) {
            List<String> regs = new ArrayList<>();
            for (int r = 0; r < registersPerRequest; r++) {
                int idx = ((i * 7 + r * 13) % REGISTER_COUNT) + 1;
                regs.add(String.format("R%03d", idx));
            }
            sets.add(regs);
        }

        // Warmup (1 iter каждой стратегии)
        flowMultiIn(c, sets.get(0));
        flowSplit(c, sets.get(0));

        // Strategy MULTI-IN
        long[] tMulti = new long[iterations];
        for (int i = 0; i < iterations; i++) {
            long t = System.nanoTime();
            flowMultiIn(c, sets.get(i + 1));
            tMulti[i] = System.nanoTime() - t;
        }

        // Strategy SPLIT
        long[] tSplit = new long[iterations];
        for (int i = 0; i < iterations; i++) {
            long t = System.nanoTime();
            flowSplit(c, sets.get(i + 1));
            tSplit[i] = System.nanoTime() - t;
        }

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("registersPerRequest", registersPerRequest);
        r.put("iterations", iterations);
        r.put("multi_in",  statsOf(tMulti));
        r.put("split",     statsOf(tSplit));
        r.put("speedup_split_over_multi",
                round(stat(tMulti, 0.50) / stat(tSplit, 0.50), 2));
        return r;
    }

    /** Стратегия (a): один SQL с WHERE REGISTER IN (?,?,...?). */
    private void flowMultiIn(IgniteClient c, List<String> regs) {
        Date fromDate = Date.valueOf(TODAY.minusDays(DAYS_BACK));
        Date toDate = Date.valueOf(TODAY);
        String placeholders = String.join(",", java.util.Collections.nCopies(regs.size(), "?"));

        // 1. REGISTER batch
        Object[] regArgs = regs.toArray();
        execBatch(c, "SELECT OBJECTID, CURRENCY, CCBALANCERECALCDATE FROM REGISTER " +
                "WHERE OBJECTID IN (" + placeholders + ")", regArgs);

        // 2. DAYBALANCES batch
        Object[] dbArgs = concat(regArgs, fromDate, toDate);
        execBatch(c, "SELECT REGISTER, CCOPERATIONDAY, CCSTARTSUM, CCDTSUM, CCKTSUM, CCFINISHSUM " +
                "FROM DAYBALANCES WHERE REGISTER IN (" + placeholders + ") " +
                "AND CCOPERATIONDAY BETWEEN ? AND ?", dbArgs);

        // 3. SUM aggregates batch
        Object[] sumArgs = concat(regArgs, fromDate, toDate);
        execBatch(c, "SELECT REGISTER, " +
                "COALESCE(SUM(CASE WHEN CCDT='1' THEN -1*CCSUM ELSE CCSUM END), 0), " +
                "COALESCE(SUM(CASE WHEN CCDT='1' THEN -1*CCSUMNAT ELSE CCSUMNAT END), 0) " +
                "FROM TURNDOCCUR WHERE REGISTER IN (" + placeholders + ") " +
                "AND CCTYPEOPER IN (0,40) AND CCOPERATIONDAY BETWEEN ? AND ? GROUP BY REGISTER", sumArgs);

        // 4. type-50 batch
        Object[] t50Args = concat(regArgs, fromDate, toDate);
        execBatch(c, "SELECT REGISTER, CCSTARTSUM, CCSTARTSUMNAT, CCOPERATIONDAY FROM TURNDOCCUR " +
                "WHERE REGISTER IN (" + placeholders + ") AND CCTYPEOPER=50 " +
                "AND CCOPERATIONDAY BETWEEN ? AND ?", t50Args);

        // 5. paginated list — не делаем batch (LIMIT per-register не сводится), пропускаем
    }

    /** Стратегия (b): N independent SQL queries по одному register'у. */
    private void flowSplit(IgniteClient c, List<String> regs) {
        for (String reg : regs) {
            singleSummary(c, reg);
        }
    }

    private static Object[] concat(Object[] arr, Object... extra) {
        Object[] r = new Object[arr.length + extra.length];
        System.arraycopy(arr, 0, r, 0, arr.length);
        System.arraycopy(extra, 0, r, arr.length, extra.length);
        return r;
    }

    private void execBatch(IgniteClient c, String sql, Object[] args) {
        try (var cur = c.query(new SqlFieldsQuery(sql).setArgs(args))) {
            for (var ignored : cur) {}
        }
    }

    private Map<String, Object> statsOf(long[] sortedMaybe) {
        long[] s = sortedMaybe.clone();
        Arrays.sort(s);
        long sum = 0;
        for (long t : s) sum += t;
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("avgMs", round(sum / (double) s.length / 1_000_000.0, 2));
        r.put("p50Ms", round(stat(s, 0.50), 2));
        r.put("p95Ms", round(stat(s, 0.95), 2));
        r.put("p99Ms", round(stat(s, 0.99), 2));
        r.put("maxMs", round(s[s.length - 1] / 1_000_000.0, 2));
        return r;
    }

    private static double stat(long[] sortedOrNot, double q) {
        long[] s = sortedOrNot;
        if (s.length == 0) return 0;
        // ensure sorted
        long[] copy = s.clone();
        Arrays.sort(copy);
        int idx = Math.min(copy.length - 1, (int) Math.ceil(q * copy.length) - 1);
        return copy[Math.max(0, idx)] / 1_000_000.0;
    }

    @PostMapping("/summary")
    public Map<String, Object> summary(
            @RequestParam(defaultValue = "500")  int iterations,
            @RequestParam(defaultValue = "10")   int concurrency,
            @RequestParam(defaultValue = "cluster-1") String clusterId) {

        IgniteClient client = clientFactory.get(clusterId);
        if (client == null) {
            return Map.of("ok", false, "error", "cluster not connected: " + clusterId);
        }

        // Pool работников
        ExecutorService pool = Executors.newFixedThreadPool(concurrency, r -> {
            Thread t = new Thread(r, "summary-perf-worker");
            t.setDaemon(true);
            return t;
        });

        // Warmup: 5 запросов в одиночку (JIT прогрев)
        for (int i = 0; i < 5; i++) singleSummary(client, registerOf(i));

        long[] timings = new long[iterations];
        AtomicInteger ok = new AtomicInteger();
        AtomicInteger err = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(iterations);

        long t0 = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            final int idx = i;
            final String reg = registerOf(i);
            pool.submit(() -> {
                long start = System.nanoTime();
                try {
                    singleSummary(client, reg);
                    timings[idx] = System.nanoTime() - start;
                    ok.incrementAndGet();
                } catch (Exception e) {
                    timings[idx] = System.nanoTime() - start;
                    err.incrementAndGet();
                    log.warn("summary perf err reg={}: {}", reg, e.toString());
                } finally {
                    done.countDown();
                }
            });
        }

        try { done.await(10, TimeUnit.MINUTES); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        long t1 = System.nanoTime();
        pool.shutdownNow();

        long[] sorted = timings.clone();
        Arrays.sort(sorted);
        long sum = 0; for (long t : sorted) sum += t;
        double avgMs = sum / (double) iterations / 1_000_000.0;

        double totalSec = (t1 - t0) / 1_000_000_000.0;
        double throughput = iterations / totalSec;

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("cluster", clusterId);
        r.put("iterations", iterations);
        r.put("concurrency", concurrency);
        r.put("ok", ok.get());
        r.put("err", err.get());
        r.put("totalSec", round(totalSec, 2));
        r.put("throughputRps", round(throughput, 1));
        r.put("avgMs", round(avgMs, 2));
        r.put("p50Ms", percentile(sorted, 0.50));
        r.put("p95Ms", percentile(sorted, 0.95));
        r.put("p99Ms", percentile(sorted, 0.99));
        r.put("p999Ms", percentile(sorted, 0.999));
        r.put("maxMs", round(sorted[sorted.length - 1] / 1_000_000.0, 2));
        return r;
    }

    private void singleSummary(IgniteClient client, String registerId) {
        Date fromDate = Date.valueOf(TODAY.minusDays(DAYS_BACK));
        Date toDate = Date.valueOf(TODAY);

        // 1. Register lookup (point по PK)
        exec(client,
                "SELECT OBJECTID, CURRENCY, CCBALANCERECALCDATE FROM REGISTER WHERE OBJECTID=?",
                registerId);

        // 2. Day balances (range через composite PK)
        exec(client,
                "SELECT REGISTER, CCOPERATIONDAY, CCSTARTSUM, CCDTSUM, CCKTSUM, CCFINISHSUM " +
                        "FROM DAYBALANCES WHERE REGISTER=? AND CCOPERATIONDAY BETWEEN ? AND ?",
                registerId, fromDate, toDate);

        // 3. Sum aggregates (index seek через IDX_TDC_REG_TYPE_OP)
        exec(client,
                "SELECT COALESCE(SUM(CASE WHEN CCDT='1' THEN -1*CCSUM ELSE CCSUM END), 0), " +
                        "       COALESCE(SUM(CASE WHEN CCDT='1' THEN -1*CCSUMNAT ELSE CCSUMNAT END), 0) " +
                        "FROM TURNDOCCUR WHERE REGISTER=? AND CCTYPEOPER IN (0,40) " +
                        "AND CCOPERATIONDAY BETWEEN ? AND ?",
                registerId, fromDate, toDate);

        // 4. Type-50 lookup (index seek через IDX_TDC_REG_TYPE_OP)
        exec(client,
                "SELECT CCSTARTSUM, CCSTARTSUMNAT, CCOPERATIONDAY FROM TURNDOCCUR " +
                        "WHERE REGISTER=? AND CCTYPEOPER=50 AND CCOPERATIONDAY BETWEEN ? AND ?",
                registerId, fromDate, toDate);

        // 5. Paginated list of operations (range + sort)
        exec(client,
                "SELECT OBJECTID, CCRQUID, CCSUM, CCOPERATIONDAY, CCDT FROM TURNDOCCUR " +
                        "WHERE REGISTER=? AND CCOPERATIONDAY BETWEEN ? AND ? " +
                        "ORDER BY CCOPERATIONDAY DESC LIMIT 100",
                registerId, fromDate, toDate);
    }

    private List<List<?>> exec(IgniteClient client, String sql, Object... args) {
        List<List<?>> rows = new ArrayList<>();
        try (var cur = client.query(new SqlFieldsQuery(sql).setArgs(args))) {
            for (List<?> row : cur) rows.add(row);
        }
        return rows;
    }

    private static String registerOf(int i) {
        return String.format("R%03d", (i % REGISTER_COUNT) + 1);
    }

    private static double percentile(long[] sorted, double q) {
        int idx = Math.min(sorted.length - 1, (int) Math.ceil(q * sorted.length) - 1);
        return round(sorted[Math.max(0, idx)] / 1_000_000.0, 2);
    }

    private static double round(double v, int decimals) {
        double f = Math.pow(10, decimals);
        return Math.round(v * f) / f;
    }
}
