package ru.sbrf.pprb.stmnt.consistency.rpc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.springframework.web.bind.annotation.*;
import ru.sbrf.pprb.stmnt.consistency.config.ConsistencyProperties;
import ru.sbrf.pprb.stmnt.consistency.integration.ignite.IgniteClientFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Debug-only endpoints for seeding identical data into all 3 clusters
 * via thin client. Useful for smoke tests.
 */
@Slf4j
@RestController
@RequestMapping("/api/debug")
@RequiredArgsConstructor
public class DebugSeedController {

    private final IgniteClientFactory clientFactory;
    private final ConsistencyProperties props;

    /**
     * Seed identical baseline data into every cluster. If body.divergeCluster
     * is non-null, that cluster gets a slightly different CURRENCY for R001
     * (so consistency check should report a mismatch).
     */
    @PostMapping("/seed")
    public Map<String, Object> seed(@RequestBody(required = false) Map<String, Object> body) {
        String divergeCluster = body == null ? null : (String) body.get("divergeCluster");
        Map<String, Object> result = new HashMap<>();
        for (var c : props.getClusters()) {
            String id = c.getId();
            IgniteClient client = clientFactory.get(id);
            if (client == null) {
                result.put(id, Map.of("ok", false, "error", "not connected"));
                continue;
            }
            try {
                String currencyForR001 = id.equals(divergeCluster) ? "EUR" : "RUB";
                seedOne(client, currencyForR001);
                result.put(id, Map.of("ok", true, "R001_currency", currencyForR001));
            } catch (Exception e) {
                log.error("seed cluster={} failed", id, e);
                result.put(id, Map.of("ok", false, "error", e.toString()));
            }
        }
        return result;
    }

    private void seedOne(IgniteClient client, String currencyForR001) {
        // Drop legacy PUBLIC.* tables from earlier debug runs (no-op if absent).
        for (String t : List.of("REGISTER", "CURRENCY", "DAYBALANCES")) {
            try { exec(client, "DROP TABLE IF EXISTS PUBLIC." + t); }
            catch (Exception ignored) {}
        }

        exec(client,
                "CREATE TABLE IF NOT EXISTS REGISTER (" +
                        "  OBJECTID VARCHAR PRIMARY KEY, CCRQTM TIMESTAMP," +
                        "  CCOPENDATE DATE, CCCLOSEDATE DATE," +
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
        execArgs(client, "INSERT INTO DAYBALANCES VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                "R001", java.sql.Date.valueOf("2026-05-24"),
                bd("1000.00"), bd("1000.00"), bd("200.00"), bd("200.00"),
                bd("500.00"), bd("500.00"), 3, 2, bd("1300.00"), bd("1300.00"));
        execArgs(client, "INSERT INTO DAYBALANCES VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                "R002", java.sql.Date.valueOf("2026-05-24"),
                bd("500.00"), bd("45000.00"), bd("100.00"), bd("9000.00"),
                bd("0.00"), bd("0.00"), 1, 0, bd("400.00"), bd("36000.00"));
    }

    private static java.math.BigDecimal bd(String s) { return new java.math.BigDecimal(s); }

    private void exec(IgniteClient c, String sql) {
        try (var cur = c.query(new SqlFieldsQuery(sql))) { cur.iterator().forEachRemaining(r -> {}); }
    }

    private void execArgs(IgniteClient c, String sql, Object... args) {
        try (var cur = c.query(new SqlFieldsQuery(sql).setArgs(args))) {
            cur.iterator().forEachRemaining(r -> {});
        }
    }

    /** Clean every cache that we know about — useful for re-seeding. */
    @PostMapping("/reset")
    public Map<String, Object> reset() {
        Map<String, Object> res = new HashMap<>();
        for (var c : props.getClusters()) {
            String id = c.getId();
            IgniteClient client = clientFactory.get(id);
            if (client == null) { res.put(id, "not connected"); continue; }
            try {
                exec(client, "DELETE FROM REGISTER");
                exec(client, "DELETE FROM CURRENCY");
                exec(client, "DELETE FROM DAYBALANCES");
                res.put(id, "cleared");
            } catch (Exception e) {
                res.put(id, "err: " + e);
            }
        }
        return res;
    }
}
