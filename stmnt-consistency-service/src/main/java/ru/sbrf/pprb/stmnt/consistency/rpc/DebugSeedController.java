package ru.sbrf.pprb.stmnt.consistency.rpc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.springframework.web.bind.annotation.*;
import ru.sbrf.pprb.stmnt.consistency.config.ConsistencyProperties;
import ru.sbrf.pprb.stmnt.consistency.integration.ignite.IgniteClientFactory;
import ru.sbrf.pprb.stmnt.consistency.lib.ErrorRegistry;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Debug-only endpoints for seeding identical data into all 3 clusters
 * and intentionally injecting various mismatch scenarios.
 */
@Slf4j
@RestController
@RequestMapping("/api/debug")
@RequiredArgsConstructor
public class DebugSeedController {

    private final IgniteClientFactory clientFactory;
    private final ConsistencyProperties props;
    private final ErrorRegistry errors;

    /** Baseline seed: identical rich dataset on every cluster. */
    @PostMapping("/seed")
    public Map<String, Object> seed(@RequestBody(required = false) Map<String, Object> body) {
        String divergeCluster = body == null ? null : (String) body.get("divergeCluster");
        Map<String, Object> result = new HashMap<>();
        for (var c : props.getClusters()) {
            String id = c.getId();
            IgniteClient client = clientFactory.get(id);
            if (client == null) { result.put(id, Map.of("ok", false, "error", "not connected")); continue; }
            try {
                seedOne(client, id.equals(divergeCluster) ? "EUR" : "RUB");
                result.put(id, Map.of("ok", true, "R001_currency",
                        id.equals(divergeCluster) ? "EUR" : "RUB"));
            } catch (Exception e) {
                log.error("seed cluster={} failed", id, e);
                errors.error("debug_seed", "SEED_FAILED",
                        "Seed failed on " + id, e, Map.of("cluster", id));
                result.put(id, Map.of("ok", false, "error", e.toString()));
            }
        }
        return result;
    }

    /**
     * Apply one of named mismatch scenarios. Idempotent — re-seeds baseline first.
     * scenarios:
     *   "currency"    — cluster-1 has CURRENCY R001=EUR; cluster-2/3 RUB.   (REGISTER mismatch)
     *   "scale"       — cluster-1 stores ccSum=200.0; cluster-2 200.000.    (DAY_BALANCES no mismatch — scale-normalized)
     *   "tolerance"   — cluster-1 has 1300.004; cluster-2 1300.003.         (within 0.01 tolerance → no mismatch)
     *   "tolerance_break" — cluster-1 has 1300.05; cluster-2 1300.00.       (> 0.01 → mismatch)
     *   "missing"     — cluster-2 missing R002 in REGISTER.                 (MISSING vs hash mismatch)
     *   "extra"       — cluster-3 has extra R999 in REGISTER.                (one-sided mismatch)
     *   "client_inn"  — CLIENT.ccINN differs on cluster-3 for C001.
     *   "cbrate"      — CB_RATE for USD differs by date on cluster-2.
     */
    @PostMapping("/scenario/{name}")
    public Map<String, Object> applyScenario(@PathVariable String name) {
        Map<String, Object> result = new HashMap<>();
        // Re-seed baseline first to a known state.
        seed(Map.of());
        for (var c : props.getClusters()) {
            String id = c.getId();
            IgniteClient client = clientFactory.get(id);
            if (client == null) { result.put(id, "not connected"); continue; }
            try {
                applyScenarioOne(client, id, name);
                result.put(id, "applied");
            } catch (Exception e) {
                errors.error("debug_seed", "SCENARIO_FAILED",
                        "Scenario " + name + " on " + id, e,
                        Map.of("cluster", id, "scenario", name));
                result.put(id, "err: " + e);
            }
        }
        return Map.of("scenario", name, "perCluster", result);
    }

    /**
     * Manual EXPLAIN — useful for ad-hoc inspection of query plans on each cluster.
     * Body: { "sql": "...", "clusterId": "cluster-1" (optional → all) }
     */
    @PostMapping("/explain")
    public Map<String, Object> explain(@RequestBody Map<String, Object> body) {
        String sql = (String) body.get("sql");
        String only = (String) body.get("clusterId");
        if (sql == null || sql.isBlank()) {
            return Map.of("error", "missing 'sql' field");
        }
        Map<String, Object> result = new HashMap<>();
        for (var c : props.getClusters()) {
            if (only != null && !only.equals(c.getId())) continue;
            IgniteClient client = clientFactory.get(c.getId());
            if (client == null) {
                result.put(c.getId(), "not connected");
                continue;
            }
            try (var cursor = client.query(new SqlFieldsQuery("EXPLAIN " + sql))) {
                java.util.List<String> lines = new java.util.ArrayList<>();
                for (List<?> row : cursor) lines.add(String.valueOf(row.get(0)));
                result.put(c.getId(), lines);
            } catch (Exception e) {
                result.put(c.getId(), "error: " + e);
            }
        }
        return Map.of("sql", sql, "plans", result);
    }

    /**
     * Affinity probe — для каждого кластера:
     *   - server-side информация: число живых server-узлов, версия Ignite
     *   - cache layout: какие кеши есть, какой affinity-mapping (через системные view'ы)
     *   - co-location check: для каждого register из REGISTER считаем partition в
     *     REGISTER / TURN_DOC_CUR / DAY_BALANCES; ожидаем одинаковый partition
     *     внутри одного кластера (за счёт @AffinityKeyMapped register).
     *
     * В docker-compose стенде каждый кластер single-node, поэтому partition внутри
     * кластера всегда тот же узел. Полезность фичи — в реальной multi-node инсталляции.
     */
    @GetMapping("/affinity")
    public Map<String, Object> affinity() {
        Map<String, Object> result = new LinkedHashMap<>();
        for (var c : props.getClusters()) {
            IgniteClient client = clientFactory.get(c.getId());
            if (client == null) {
                result.put(c.getId(), Map.of("ok", false, "error", "not connected"));
                continue;
            }
            Map<String, Object> per = new LinkedHashMap<>();
            try {
                // 1. cluster size / nodes
                int nodes = client.cluster().nodes().size();
                per.put("server_nodes", nodes);

                // 2. cache list (filter PUBLIC-style — наши таблицы)
                var caches = client.cacheNames();
                per.put("caches", caches);

                // 3. SYS.NODES — server endpoints (thin client compatible)
                java.util.List<Map<String, Object>> nodeRows = new java.util.ArrayList<>();
                try (var cur = client.query(new SqlFieldsQuery(
                        "SELECT NODE_ID, IS_CLIENT, NODE_ORDER FROM SYS.NODES"))) {
                    for (List<?> row : cur) {
                        nodeRows.add(Map.of(
                                "node_id",     String.valueOf(row.get(0)),
                                "is_client",   row.get(1),
                                "node_order",  row.get(2)));
                    }
                }
                per.put("sys_nodes", nodeRows);

                // 4. CACHES — backups / mode / affinity (Ignite 2.16 SYS view).
                // Ignite 2.16 SYS.CACHES не имеет AFFINITY_KEY_TYPE; используем AFFINITY
                // (текстовое представление AffinityFunction) и CACHE_GROUP_NAME.
                java.util.List<Map<String, Object>> cacheRows = new java.util.ArrayList<>();
                try (var cur = client.query(new SqlFieldsQuery(
                        "SELECT CACHE_NAME, CACHE_GROUP_NAME, BACKUPS, CACHE_MODE, AFFINITY " +
                                "FROM SYS.CACHES " +
                                "WHERE CACHE_NAME IN ('REGISTER','TURN_DOC_CUR','DAY_BALANCES'," +
                                "                    'CURRENCY','CB_RATE','CLIENT')"))) {
                    for (List<?> row : cur) {
                        cacheRows.add(Map.of(
                                "cache_name",     String.valueOf(row.get(0)),
                                "cache_group",    String.valueOf(row.get(1)),
                                "backups",        row.get(2),
                                "cache_mode",     String.valueOf(row.get(3)),
                                "affinity_func",  String.valueOf(row.get(4))));
                    }
                }
                per.put("sys_caches", cacheRows);

                // 4b. CACHE_GROUPS — реальное число партиций и режим (PARTITIONS живёт здесь)
                java.util.List<Map<String, Object>> groupRows = new java.util.ArrayList<>();
                try (var cur = client.query(new SqlFieldsQuery(
                        "SELECT CACHE_GROUP_NAME, PARTITIONS_COUNT, DATA_REGION_NAME " +
                                "FROM SYS.CACHE_GROUPS"))) {
                    for (List<?> row : cur) {
                        String gn = String.valueOf(row.get(0));
                        if ("ignite-sys-cache".equals(gn) || "default".equals(gn)) continue;
                        groupRows.add(Map.of(
                                "cache_group",      gn,
                                "partitions",       row.get(1),
                                "data_region",      String.valueOf(row.get(2))));
                    }
                }
                per.put("sys_cache_groups", groupRows);

                // 5. co-location probe: для всех REGISTER считаем partition по каждому кешу
                //    (через CACHE() helper)
                java.util.List<Map<String, Object>> colocate = new java.util.ArrayList<>();
                try (var cur = client.query(new SqlFieldsQuery(
                        "SELECT OBJECTID FROM REGISTER LIMIT 5"))) {
                    for (List<?> row : cur) {
                        String reg = String.valueOf(row.get(0));
                        Integer pReg     = partitionOf(client, "REGISTER",      reg);
                        Integer pTurn    = partitionOf(client, "TURN_DOC_CUR",  reg);
                        Integer pDayB    = partitionOf(client, "DAY_BALANCES", reg);
                        boolean ok = (pReg != null && pReg.equals(pTurn) && pReg.equals(pDayB));
                        colocate.add(Map.of(
                                "register",         reg,
                                "REGISTER_part",    String.valueOf(pReg),
                                "TURN_DOC_CUR_part", String.valueOf(pTurn),
                                "DAY_BALANCES_part", String.valueOf(pDayB),
                                "collocated_ok",    ok));
                    }
                }
                per.put("colocation_by_partition", colocate);

                // 5b. Колокация через cache_group — критерий для true colocated-joins.
                // Если два кеша в одной cache_group + используют одинаковый affinity-mapping,
                // INNER JOIN по REGISTER можно делать collocated (без cross-partition shuffle).
                Map<String, Object> groupOf = new LinkedHashMap<>();
                for (Map<String, Object> r : cacheRows) {
                    groupOf.put((String) r.get("cache_name"), r.get("cache_group"));
                }
                boolean sameGroup =
                        groupOf.get("REGISTER") != null
                                && groupOf.get("REGISTER").equals(groupOf.get("TURN_DOC_CUR"))
                                && groupOf.get("REGISTER").equals(groupOf.get("DAY_BALANCES"));
                per.put("colocation_by_cache_group_ok", sameGroup);
                per.put("note", sameGroup
                        ? "REGISTER/TURN_DOC_CUR/DAY_BALANCES share one cache_group — collocated JOINs work."
                        : "Each cache has its own cache_group (SQL-DDL default). For production set " +
                          "single cache_group OR same affinity_key on all caches.");
                per.put("ok", true);
            } catch (Exception e) {
                per.put("ok", false);
                per.put("error", e.toString());
            }
            result.put(c.getId(), per);
        }
        return result;
    }

    /**
     * Partition number for an affinity key using Ignite SYS.CACHE_GROUPS (Ignite 2.16
     * does not expose PARTITIONS on SYS.CACHES, only on SYS.CACHE_GROUPS).
     *
     * NOTE: this is a CRUDE approximation — real RendezvousAffinityFunction is not
     * a simple hashCode%N. For demonstration only. For single-node clusters it
     * doesn't matter (everything is on one node anyway).
     */
    private Integer partitionOf(IgniteClient client, String cacheName, String affKey) {
        try (var cur = client.query(new SqlFieldsQuery(
                "SELECT cg.PARTITIONS_COUNT FROM SYS.CACHES c " +
                        " JOIN SYS.CACHE_GROUPS cg ON c.CACHE_GROUP_NAME = cg.CACHE_GROUP_NAME " +
                        " WHERE c.CACHE_NAME = ?").setArgs(cacheName))) {
            for (List<?> row : cur) {
                Object v = row.get(0);
                if (v == null) continue;
                int parts = ((Number) v).intValue();
                // affKey.hashCode() — НЕ настоящий RendezvousAffinityFunction; для приближения хватит.
                return Math.floorMod(affKey.hashCode(), parts);
            }
        } catch (Exception ignored) {}
        return null;
    }

    @PostMapping("/reset")
    public Map<String, Object> reset() {
        Map<String, Object> res = new HashMap<>();
        for (var c : props.getClusters()) {
            IgniteClient client = clientFactory.get(c.getId());
            if (client == null) { res.put(c.getId(), "not connected"); continue; }
            try {
                exec(client, "DELETE FROM REGISTER");
                exec(client, "DELETE FROM CURRENCY");
                exec(client, "DELETE FROM DAYBALANCES");
                exec(client, "DELETE FROM CLIENT");
                exec(client, "DELETE FROM CBRATE");
                res.put(c.getId(), "cleared");
            } catch (Exception e) { res.put(c.getId(), "err: " + e); }
        }
        return res;
    }

    // -------- internals -------------------------------------------------------

    private void seedOne(IgniteClient client, String currencyForR001) {
        // Drop legacy tables (no-op if absent)
        for (String t : List.of("REGISTER", "CURRENCY", "DAYBALANCES", "CLIENT", "CBRATE")) {
            try { exec(client, "DROP TABLE IF EXISTS PUBLIC." + t); } catch (Exception ignored) {}
        }

        // ---- REGISTER ----
        exec(client,
                "CREATE TABLE IF NOT EXISTS REGISTER (" +
                        "  OBJECTID VARCHAR PRIMARY KEY, CCRQTM TIMESTAMP, CCOPENDATE DATE, CCCLOSEDATE DATE," +
                        "  CURRENCY VARCHAR, CCBALANCERECALCDATE DATE" +
                        ") WITH \"CACHE_NAME=REGISTER, VALUE_TYPE=Register\"");
        exec(client, "DELETE FROM REGISTER");
        execArgs(client,
                "INSERT INTO REGISTER (OBJECTID, CCRQTM, CCOPENDATE, CURRENCY, CCBALANCERECALCDATE) " +
                        "VALUES (?, TIMESTAMP '2026-01-15 12:00:00', DATE '2020-01-01', ?, DATE '2026-05-24')",
                "R001", currencyForR001);
        execArgs(client,
                "INSERT INTO REGISTER (OBJECTID, CCRQTM, CCOPENDATE, CURRENCY, CCBALANCERECALCDATE) " +
                        "VALUES (?, TIMESTAMP '2026-01-16 12:00:00', DATE '2021-06-01', ?, DATE '2026-05-24')",
                "R002", "USD");
        execArgs(client,
                "INSERT INTO REGISTER (OBJECTID, CCRQTM, CCOPENDATE, CURRENCY, CCBALANCERECALCDATE) " +
                        "VALUES (?, TIMESTAMP '2026-02-01 09:30:00', DATE '2022-01-15', ?, DATE '2026-05-24')",
                "R003", "EUR");

        // ---- CURRENCY ----
        exec(client,
                "CREATE TABLE IF NOT EXISTS CURRENCY (" +
                        "  OBJECTID VARCHAR PRIMARY KEY, CCEKSCODE VARCHAR, CCEXTCODE VARCHAR," +
                        "  CCNAME VARCHAR, CCALPHACODE VARCHAR, CCALPHANUMCODE VARCHAR" +
                        ") WITH \"CACHE_NAME=CURRENCY, VALUE_TYPE=Currency\"");
        exec(client, "DELETE FROM CURRENCY");
        execArgs(client, "INSERT INTO CURRENCY VALUES (?,?,?,?,?,?)",
                "CUR_RUB", "810", "810", "Российский рубль", "RUB", "643");
        execArgs(client, "INSERT INTO CURRENCY VALUES (?,?,?,?,?,?)",
                "CUR_USD", "840", "840", "Доллар США", "USD", "840");
        execArgs(client, "INSERT INTO CURRENCY VALUES (?,?,?,?,?,?)",
                "CUR_EUR", "978", "978", "Евро", "EUR", "978");

        // ---- DAY_BALANCES ----
        exec(client,
                "CREATE TABLE IF NOT EXISTS DAYBALANCES (" +
                        "  REGISTER VARCHAR, CCOPERATIONDAY DATE," +
                        "  CCSTARTSUM DECIMAL(20,6), CCSTARTSUMNAT DECIMAL(20,6)," +
                        "  CCDTSUM DECIMAL(20,6), CCDTSUMNAT DECIMAL(20,6)," +
                        "  CCKTSUM DECIMAL(20,6), CCKTSUMNAT DECIMAL(20,6)," +
                        "  CCDTCOUNT INT, CCKTCOUNT INT," +
                        "  CCFINISHSUM DECIMAL(20,6), CCFINISHSUMNAT DECIMAL(20,6)," +
                        "  PRIMARY KEY (REGISTER, CCOPERATIONDAY)" +
                        ") WITH \"CACHE_NAME=DAY_BALANCES, VALUE_TYPE=DayBalances\"");
        exec(client, "DELETE FROM DAYBALANCES");
        // Multiple days for R001
        for (int d = 22; d <= 24; d++) {
            execArgs(client, "INSERT INTO DAYBALANCES VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    "R001", Date.valueOf("2026-05-" + d),
                    bd(1000.0 * d), bd(1000.0 * d),
                    bd(200.0), bd(200.0), bd(500.0), bd(500.0),
                    3, 2, bd(1000.0 * d - 200.0 + 500.0), bd(1000.0 * d - 200.0 + 500.0));
        }
        execArgs(client, "INSERT INTO DAYBALANCES VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                "R002", Date.valueOf("2026-05-24"),
                bd("500.00"), bd("45000.00"), bd("100.00"), bd("9000.00"),
                bd("0.00"), bd("0.00"), 1, 0, bd("400.00"), bd("36000.00"));

        // ---- CLIENT ----
        exec(client,
                "CREATE TABLE IF NOT EXISTS CLIENT (" +
                        "  OBJECTID VARCHAR PRIMARY KEY, CCEPK VARCHAR, CCRQTM TIMESTAMP," +
                        "  CCNAME VARCHAR, CCINN VARCHAR" +
                        ") WITH \"CACHE_NAME=CLIENT, VALUE_TYPE=Client\"");
        exec(client, "DELETE FROM CLIENT");
        execArgs(client, "INSERT INTO CLIENT VALUES (?,?,?,?,?)",
                "C001", "EPK-111", java.sql.Timestamp.valueOf("2026-01-01 10:00:00"),
                "ООО Альфа", "7700111111");
        execArgs(client, "INSERT INTO CLIENT VALUES (?,?,?,?,?)",
                "C002", "EPK-222", java.sql.Timestamp.valueOf("2026-01-02 10:00:00"),
                "ООО Бета", "7700222222");

        // ---- CB_RATE ----
        exec(client,
                "CREATE TABLE IF NOT EXISTS CBRATE (" +
                        "  OBJECTID VARCHAR PRIMARY KEY, CCCODE VARCHAR, CCDATE DATE," +
                        "  CCRATE DECIMAL(20,6), CCLOTSIZE DECIMAL(20,6)" +
                        ") WITH \"CACHE_NAME=CB_RATE, VALUE_TYPE=CbRate\"");
        exec(client, "DELETE FROM CBRATE");
        execArgs(client, "INSERT INTO CBRATE VALUES (?,?,?,?,?)",
                "USD_20260524", "USD", Date.valueOf("2026-05-24"), bd("90.50"), bd("1"));
        execArgs(client, "INSERT INTO CBRATE VALUES (?,?,?,?,?)",
                "EUR_20260524", "EUR", Date.valueOf("2026-05-24"), bd("98.20"), bd("1"));
    }

    private void applyScenarioOne(IgniteClient c, String clusterId, String scenario) {
        switch (scenario) {
            case "currency" -> {
                if ("cluster-1".equals(clusterId))
                    exec(c, "UPDATE REGISTER SET CURRENCY='EUR' WHERE OBJECTID='R001'");
            }
            case "scale" -> {
                // BigDecimal scale variants must produce IDENTICAL hash (HashUtil normalizes to scale=6).
                // No actual change to expected behavior.
                if ("cluster-1".equals(clusterId)) {
                    execArgs(c,
                            "UPDATE DAYBALANCES SET CCDTSUM=? WHERE REGISTER='R001' AND CCOPERATIONDAY=?",
                            new BigDecimal("200.0"), Date.valueOf("2026-05-24"));
                } else if ("cluster-2".equals(clusterId)) {
                    execArgs(c,
                            "UPDATE DAYBALANCES SET CCDTSUM=? WHERE REGISTER='R001' AND CCOPERATIONDAY=?",
                            new BigDecimal("200.000000"), Date.valueOf("2026-05-24"));
                }
            }
            case "tolerance" -> {
                // Within DAY_BALANCES 0.01 tolerance — pre-hashes are rounded to 2 decimals.
                if ("cluster-1".equals(clusterId)) {
                    execArgs(c,
                            "UPDATE DAYBALANCES SET CCFINISHSUM=? WHERE REGISTER='R001' AND CCOPERATIONDAY=?",
                            new BigDecimal("24300.004"), Date.valueOf("2026-05-24"));
                } else if ("cluster-2".equals(clusterId)) {
                    execArgs(c,
                            "UPDATE DAYBALANCES SET CCFINISHSUM=? WHERE REGISTER='R001' AND CCOPERATIONDAY=?",
                            new BigDecimal("24300.003"), Date.valueOf("2026-05-24"));
                }
            }
            case "tolerance_break" -> {
                // Beyond 0.01 tolerance — hash differs after rounding.
                if ("cluster-1".equals(clusterId)) {
                    execArgs(c,
                            "UPDATE DAYBALANCES SET CCFINISHSUM=? WHERE REGISTER='R001' AND CCOPERATIONDAY=?",
                            new BigDecimal("24300.06"), Date.valueOf("2026-05-24"));
                }
            }
            case "missing" -> {
                if ("cluster-2".equals(clusterId))
                    exec(c, "DELETE FROM REGISTER WHERE OBJECTID='R002'");
            }
            case "extra" -> {
                if ("cluster-3".equals(clusterId)) {
                    execArgs(c,
                            "INSERT INTO REGISTER (OBJECTID, CCRQTM, CCOPENDATE, CURRENCY, CCBALANCERECALCDATE) " +
                                    "VALUES (?, TIMESTAMP '2026-03-01 12:00:00', DATE '2023-01-01', ?, DATE '2026-05-24')",
                            "R999", "RUB");
                }
            }
            case "client_inn" -> {
                if ("cluster-3".equals(clusterId))
                    exec(c, "UPDATE CLIENT SET CCINN='9999999999' WHERE OBJECTID='C001'");
            }
            case "cbrate" -> {
                if ("cluster-2".equals(clusterId))
                    execArgs(c, "UPDATE CBRATE SET CCRATE=? WHERE OBJECTID='USD_20260524'",
                            new BigDecimal("91.99"));
            }
            default -> throw new IllegalArgumentException("Unknown scenario: " + scenario);
        }
    }

    private static BigDecimal bd(String s) { return new BigDecimal(s); }
    private static BigDecimal bd(double d) { return BigDecimal.valueOf(d); }

    private void exec(IgniteClient c, String sql) {
        try (var cur = c.query(new SqlFieldsQuery(sql))) { cur.iterator().forEachRemaining(r -> {}); }
    }
    private void execArgs(IgniteClient c, String sql, Object... args) {
        try (var cur = c.query(new SqlFieldsQuery(sql).setArgs(args))) {
            cur.iterator().forEachRemaining(r -> {});
        }
    }
}
