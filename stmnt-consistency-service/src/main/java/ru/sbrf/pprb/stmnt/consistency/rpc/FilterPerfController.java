package ru.sbrf.pprb.stmnt.consistency.rpc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.springframework.web.bind.annotation.*;
import ru.sbrf.pprb.stmnt.consistency.integration.ignite.IgniteClientFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Perf-замер всех фильтров getStatementSummary в одном flow.
 *
 *   POST /api/perf/filters?register=R001&iterations=20
 *
 * 12 сценариев фильтров — каждый замеряется отдельно с p50/p95/p99/max.
 * Цель — сравнить latency разных фильтров на одном dataset'е, чтобы понять
 * какие фильтры дёшевы (индекс работает) а какие — full scan.
 */
@Slf4j
@RestController
@RequestMapping("/api/perf")
@RequiredArgsConstructor
public class FilterPerfController {

    private final IgniteClientFactory clientFactory;

    private static final LocalDate TODAY = LocalDate.of(2026, 5, 24);
    private static final Date DAY_30D_AGO = Date.valueOf(TODAY.minusDays(30));
    private static final Date DAY_90D_AGO = Date.valueOf(TODAY.minusDays(90));
    private static final Date DAY_1D_AGO = Date.valueOf(TODAY.minusDays(1));
    private static final Date DAY_TODAY = Date.valueOf(TODAY);

    @PostMapping("/filters")
    public Map<String, Object> filters(
            @RequestParam(defaultValue = "R001") String register,
            @RequestParam(defaultValue = "20") int iterations,
            @RequestParam(defaultValue = "cluster-1") String clusterId) {

        IgniteClient c = clientFactory.get(clusterId);
        if (c == null) return Map.of("ok", false, "error", "cluster not connected: " + clusterId);

        Map<String, Object> r = new LinkedHashMap<>();

        // === 1. ТОЛЬКО REGISTER (baseline) =====================================
        r.put("01_register_only", bench(c, iterations,
                "SELECT * FROM TURNDOCCUR WHERE REGISTER=?", register));

        // === 2. REGISTER + range 1 day (узкий) =================================
        r.put("02_reg_date_1day", bench(c, iterations,
                "SELECT * FROM TURNDOCCUR WHERE REGISTER=? AND CCOPERATIONDAY=?",
                register, DAY_1D_AGO));

        // === 3. REGISTER + range 30 days (средний) =============================
        r.put("03_reg_date_30days", bench(c, iterations,
                "SELECT * FROM TURNDOCCUR WHERE REGISTER=? " +
                        "AND CCOPERATIONDAY BETWEEN ? AND ?",
                register, DAY_30D_AGO, DAY_TODAY));

        // === 4. REGISTER + range 90 days (широкий) =============================
        r.put("04_reg_date_90days", bench(c, iterations,
                "SELECT * FROM TURNDOCCUR WHERE REGISTER=? " +
                        "AND CCOPERATIONDAY BETWEEN ? AND ?",
                register, DAY_90D_AGO, DAY_TODAY));

        // === 5. + Type filter ==================================================
        r.put("05_reg_date_type_in", bench(c, iterations,
                "SELECT * FROM TURNDOCCUR WHERE REGISTER=? " +
                        "AND CCOPERATIONDAY BETWEEN ? AND ? AND CCTYPEOPER IN (0,40)",
                register, DAY_30D_AGO, DAY_TODAY));

        // === 6. + Amount range =================================================
        r.put("06_reg_date_amount", bench(c, iterations,
                "SELECT * FROM TURNDOCCUR WHERE REGISTER=? " +
                        "AND CCOPERATIONDAY BETWEEN ? AND ? AND CCSUM BETWEEN ? AND ?",
                register, DAY_30D_AGO, DAY_TODAY, new BigDecimal("100"), new BigDecimal("500")));

        // === 7. + Counterparty INN (single via OR) =============================
        r.put("07_reg_date_inn_or", bench(c, iterations,
                "SELECT * FROM TURNDOCCUR WHERE REGISTER=? " +
                        "AND CCOPERATIONDAY BETWEEN ? AND ? " +
                        "AND (CCDTINN=? OR CCKTINN=?)",
                register, DAY_30D_AGO, DAY_TODAY, "7701000001", "7701000001"));

        // === 8. + Counterparty Account =========================================
        r.put("08_reg_date_acc_or", bench(c, iterations,
                "SELECT * FROM TURNDOCCUR WHERE REGISTER=? " +
                        "AND CCOPERATIONDAY BETWEEN ? AND ? " +
                        "AND (CCDTACC=? OR CCKTACC=?)",
                register, DAY_30D_AGO, DAY_TODAY,
                "40702810123456700001", "40702810123456700001"));

        // === 9. + Direction (CCDT) =============================================
        r.put("09_reg_date_dir", bench(c, iterations,
                "SELECT * FROM TURNDOCCUR WHERE REGISTER=? " +
                        "AND CCOPERATIONDAY BETWEEN ? AND ? AND CCDT=?",
                register, DAY_30D_AGO, DAY_TODAY, "1"));

        // === 10. + ALL FILTERS (real-world combo) ==============================
        r.put("10_combo_all", bench(c, iterations,
                "SELECT * FROM TURNDOCCUR WHERE REGISTER=? " +
                        "AND CCOPERATIONDAY BETWEEN ? AND ? " +
                        "AND CCTYPEOPER IN (0,40) " +
                        "AND CCSUM BETWEEN ? AND ? " +
                        "AND CCDT=?",
                register, DAY_30D_AGO, DAY_TODAY,
                new BigDecimal("100"), new BigDecimal("500"), "1"));

        // === 11. + ORDER BY DATE DESC + LIMIT (пагинация) ======================
        r.put("11_paged_ordered", bench(c, iterations,
                "SELECT * FROM TURNDOCCUR WHERE REGISTER=? " +
                        "AND CCOPERATIONDAY BETWEEN ? AND ? " +
                        "ORDER BY CCOPERATIONDAY DESC, CCDATE DESC LIMIT 50",
                register, DAY_30D_AGO, DAY_TODAY));

        // === 12. + LIMIT + OFFSET (вторая страница) ============================
        r.put("12_paged_offset", bench(c, iterations,
                "SELECT * FROM TURNDOCCUR WHERE REGISTER=? " +
                        "AND CCOPERATIONDAY BETWEEN ? AND ? " +
                        "ORDER BY CCOPERATIONDAY DESC LIMIT 50 OFFSET 100",
                register, DAY_30D_AGO, DAY_TODAY));

        // === 13. По purpose text LIKE (text search) ============================
        r.put("13_purpose_like", bench(c, iterations,
                "SELECT * FROM TURNDOCCUR WHERE REGISTER=? " +
                        "AND CCOPERATIONDAY BETWEEN ? AND ? " +
                        "AND CCPURPOSE LIKE ?",
                register, DAY_30D_AGO, DAY_TODAY, "%Зарплата%"));

        // === 14. Document number lookup ========================================
        r.put("14_by_docnum", bench(c, iterations,
                "SELECT * FROM TURNDOCCUR WHERE REGISTER=? " +
                        "AND CCOPERATIONDAY BETWEEN ? AND ? AND CCNUM=?",
                register, DAY_30D_AGO, DAY_TODAY, "000005"));

        // === 15. ccRqUId lookup (uniq) ========================================
        r.put("15_by_rquid", bench(c, iterations,
                "SELECT * FROM TURNDOCCUR WHERE CCRQUID=?",
                "RQ_" + register + ":D0:0"));

        // === 16. ccIdEKS lookup ===============================================
        r.put("16_by_ideks", bench(c, iterations,
                "SELECT * FROM TURNDOCCUR WHERE CCIDEKS=?",
                "EKS1_0_0"));

        return r;
    }

    private Map<String, Object> bench(IgniteClient c, int iters, String sql, Object... args) {
        // warmup
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
