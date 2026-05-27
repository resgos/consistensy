package ru.sbrf.pprb.stmnt.consistency.rpc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.springframework.web.bind.annotation.*;
import ru.sbrf.pprb.stmnt.consistency.config.ConsistencyProperties;
import ru.sbrf.pprb.stmnt.consistency.integration.ignite.IgniteClientFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Bulk-seed для перформанс-тестов: заливает реалистичный объём данных
 * на ВСЕ кластеры одинаково — для проверки планов запросов на datasete'е
 * где у planner'а есть выбор INDEX vs FULL_SCAN.
 *
 *   POST /api/debug/bulk-seed?registers=100&days=30&docsPerDay=10
 *
 * Делает только TURN_DOC_CUR (главная таблица), REGISTER, DAY_BALANCES
 * — то что использует hashers и DayBalancesRecalcService для range-scan.
 *
 * Через SQL INSERT (batched) — CDC сработает через IgniteEvents.localListen.
 * Опция nocdc=true делает SET STREAMING ON чтобы залить быстрее без CDC events
 * (для чистого SQL-perf теста).
 */
@Slf4j
@RestController
@RequestMapping("/api/debug")
@RequiredArgsConstructor
public class BulkSeedController {

    private final ConsistencyProperties props;
    private final IgniteClientFactory clientFactory;

    @PostMapping("/bulk-seed")
    public Map<String, Object> bulkSeed(
            @RequestParam(defaultValue = "100") int registers,
            @RequestParam(defaultValue = "30")  int days,
            @RequestParam(defaultValue = "10")  int docsPerDay,
            @RequestParam(defaultValue = "")    String clusters) {

        List<String> targets = clusters.isBlank()
                ? props.getClusters().stream().map(ConsistencyProperties.Cluster::getId).toList()
                : List.of(clusters.split(","));

        Map<String, Object> result = new LinkedHashMap<>();
        for (String cid : targets) {
            IgniteClient c = clientFactory.get(cid);
            if (c == null) { result.put(cid, Map.of("ok", false, "error", "not connected")); continue; }
            try {
                long t0 = System.currentTimeMillis();
                Stats s = seedOne(c, registers, days, docsPerDay);
                long t1 = System.currentTimeMillis();
                result.put(cid, Map.of(
                        "ok", true,
                        "registers", s.registers,
                        "turnDocs", s.turnDocs,
                        "dayBalances", s.dayBalances,
                        "elapsedMs", t1 - t0));
            } catch (Exception e) {
                log.error("bulk-seed failed cluster={}", cid, e);
                result.put(cid, Map.of("ok", false, "error", e.toString()));
            }
        }
        return Map.of("params",
                Map.of("registers", registers, "days", days, "docsPerDay", docsPerDay),
                "perCluster", result);
    }

    private static class Stats {
        int registers, turnDocs, dayBalances;
    }

    private Stats seedOne(IgniteClient client, int registers, int days, int docsPerDay) {
        Stats s = new Stats();

        // 1. REGISTER + indexes
        exec(client, "CREATE TABLE IF NOT EXISTS REGISTER ("
                + "  OBJECTID VARCHAR PRIMARY KEY, CCRQTM TIMESTAMP, CCOPENDATE DATE, CCCLOSEDATE DATE,"
                + "  CURRENCY VARCHAR, CCBALANCERECALCDATE DATE"
                + ") WITH \"CACHE_NAME=REGISTER, VALUE_TYPE=Register\"");
        // production-DTO Register.java НЕ имеет @QuerySqlField(index=true) на
        // ccBalanceRecalcDate / ccCloseDate / ccReestrRecalcDate /
        // ccDayBalancesBeginDate — это технический долг (находка НТ), задаются
        // здесь явно, чтобы планы smoke отражали target-state production'а.
        exec(client, "CREATE INDEX IF NOT EXISTS IDX_REG_BAL_RECALC ON REGISTER (CCBALANCERECALCDATE)");
        exec(client, "CREATE INDEX IF NOT EXISTS IDX_REG_CLOSE ON REGISTER (CCCLOSEDATE)");
        exec(client, "DELETE FROM REGISTER");
        Timestamp baseTm = Timestamp.valueOf("2026-01-01 00:00:00");
        Date openDate = Date.valueOf("2023-01-01");
        LocalDate today = LocalDate.now();
        for (int r = 1; r <= registers; r++) {
            String regId = String.format("R%03d", r);
            String currency = (r % 5 == 0) ? "USD" : "RUB";
            execArgs(client,
                    "INSERT INTO REGISTER (OBJECTID, CCRQTM, CCOPENDATE, CURRENCY, CCBALANCERECALCDATE) " +
                            "VALUES (?, ?, ?, ?, ?)",
                    regId, baseTm, openDate, currency, Date.valueOf(today));
            s.registers++;
        }

        // 2. TURN_DOC_CUR + indexes — расширенная схема под фильтр-НТ.
        // Добавлены поля контрагентов (ИНН/счёт), назначение, номер документа —
        // эмулирует реальный turn_doc_cur для тестов фильтров getStatementSummary.
        try { exec(client, "DROP TABLE IF EXISTS TURNDOCCUR"); } catch (Exception ignore) {}
        exec(client, "CREATE TABLE TURNDOCCUR ("
                + "  OBJECTID VARCHAR PRIMARY KEY, CCRQUID VARCHAR, CCIDEKS VARCHAR, REGISTER VARCHAR,"
                + "  CCSUM DECIMAL, CCSUMNAT DECIMAL, CCSTARTSUM DECIMAL, CCSTARTSUMNAT DECIMAL,"
                + "  CCDT VARCHAR, CCTYPEOPER DECIMAL, CCDATE DATE, CCOPERATIONDAY DATE,"
                + "  CCDTINN VARCHAR, CCKTINN VARCHAR, CCDTACC VARCHAR, CCKTACC VARCHAR,"
                + "  CCPURPOSE VARCHAR, CCNUM VARCHAR"
                + ") WITH \"CACHE_NAME=TURN_DOC_CUR, VALUE_TYPE=TurnDocCur\"");

        exec(client, "CREATE INDEX IF NOT EXISTS IDX_TDC_REG_TYPE_OP ON TURNDOCCUR "
                + "(REGISTER, CCTYPEOPER, CCOPERATIONDAY)");
        exec(client, "CREATE INDEX IF NOT EXISTS IDX_TDC_OPDAY ON TURNDOCCUR (CCOPERATIONDAY)");
        exec(client, "CREATE INDEX IF NOT EXISTS IDX_TDC_TYPE ON TURNDOCCUR (CCTYPEOPER)");
        exec(client, "CREATE INDEX IF NOT EXISTS IDX_TDC_IDEKS ON TURNDOCCUR (CCIDEKS)");
        // Индексы под фильтры (как в production):
        exec(client, "CREATE INDEX IF NOT EXISTS IDX_TDC_DTINN ON TURNDOCCUR (CCDTINN)");
        exec(client, "CREATE INDEX IF NOT EXISTS IDX_TDC_KTINN ON TURNDOCCUR (CCKTINN)");
        exec(client, "CREATE INDEX IF NOT EXISTS IDX_TDC_DTACC ON TURNDOCCUR (CCDTACC)");
        exec(client, "CREATE INDEX IF NOT EXISTS IDX_TDC_KTACC ON TURNDOCCUR (CCKTACC)");

        // Counterparty pool — 100 уникальных ИНН/счетов
        String[] innPool = new String[100];
        String[] accPool = new String[100];
        for (int i = 0; i < 100; i++) {
            innPool[i] = String.format("770%07d", 1000000 + i);   // 10-digit INN
            accPool[i] = String.format("40702810%08d000%01d", 12345670 + i, i % 10);
        }

        for (int r = 1; r <= registers; r++) {
            String regId = String.format("R%03d", r);
            for (int d = 0; d < days; d++) {
                LocalDate operDay = today.minusDays(d);
                Date sqlDay = Date.valueOf(operDay);
                for (int doc = 0; doc < docsPerDay; doc++) {
                    String objId = regId + ":D" + d + ":" + doc;
                    String idEks = "EKS" + r + "_" + d + "_" + doc;
                    String dt = (doc % 2 == 0) ? "0" : "1";
                    int typeOper = (doc % 3 == 0) ? 0 : 40;
                    BigDecimal sum = new BigDecimal((100 + doc * 10) + "." + (r % 100));
                    // Распределение counterparty: используем 100 INN, hash от
                    // (register*day*doc) для разнообразия. На register'е будет
                    // 20-30 уникальных counterpart, что реалистично.
                    int innIdx = (r * 17 + d * 31 + doc * 7) % 100;
                    int accIdx = (r * 13 + d * 23 + doc * 11) % 100;
                    String purpose = (doc % 5 == 0)
                            ? "Зарплата за " + operDay
                            : "Оплата по договору " + (1000 + (doc % 50));
                    execArgs(client,
                            "INSERT INTO TURNDOCCUR " +
                                    "(OBJECTID, CCRQUID, CCIDEKS, REGISTER, CCSUM, CCSUMNAT, CCDT, " +
                                    " CCTYPEOPER, CCDATE, CCOPERATIONDAY," +
                                    " CCDTINN, CCKTINN, CCDTACC, CCKTACC, CCPURPOSE, CCNUM) " +
                                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            objId, "RQ_" + objId, idEks, regId,
                            sum, sum, dt,
                            new BigDecimal(typeOper), sqlDay, sqlDay,
                            innPool[innIdx], innPool[(innIdx + 1) % 100],
                            accPool[accIdx], accPool[(accIdx + 1) % 100],
                            purpose, String.format("%06d", doc * 100 + d));
                    s.turnDocs++;
                }
                // +1 типа50 (начальное сальдо) на каждый (register, day) —
                // нужно для воспроизведения реальных queries из
                // GetStatementSummaryLibraryIgnite (loadOper50ContextBatch и т.п.)
                String t50Id = regId + ":D" + d + ":T50";
                BigDecimal startSum = new BigDecimal((1000 + d) + "." + (r % 100));
                execArgs(client,
                        "INSERT INTO TURNDOCCUR " +
                                "(OBJECTID, CCRQUID, CCIDEKS, REGISTER, CCSUM, CCSUMNAT, " +
                                " CCSTARTSUM, CCSTARTSUMNAT, CCDT, CCTYPEOPER, CCDATE, CCOPERATIONDAY," +
                                " CCDTINN, CCKTINN, CCDTACC, CCKTACC, CCPURPOSE, CCNUM) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        t50Id, "RQ_" + t50Id, "EKS" + r + "_" + d + "_T50", regId,
                        BigDecimal.ZERO, BigDecimal.ZERO,
                        startSum, startSum,
                        "1", new BigDecimal(50), sqlDay, sqlDay,
                        null, null, null, null, "Начальное сальдо", "T50");
                s.turnDocs++;
            }
        }

        // 3. DAY_BALANCES + indexes
        exec(client, "CREATE TABLE IF NOT EXISTS DAYBALANCES ("
                + "  REGISTER VARCHAR, CCOPERATIONDAY DATE, CCSTARTSUM DECIMAL, CCSTARTSUMNAT DECIMAL,"
                + "  CCDTSUM DECIMAL, CCDTSUMNAT DECIMAL, CCKTSUM DECIMAL, CCKTSUMNAT DECIMAL,"
                + "  CCDTCOUNT DECIMAL, CCKTCOUNT DECIMAL, CCFINISHSUM DECIMAL, CCFINISHSUMNAT DECIMAL,"
                + "  PRIMARY KEY (REGISTER, CCOPERATIONDAY)"
                + ") WITH \"CACHE_NAME=DAY_BALANCES, VALUE_TYPE=DayBalances\"");
        // отдельный индекс на ccOperationDay — для range-сканов WHERE ccOperationDay >= ?
        // (PK index по (register, ccOperationDay) такие range не покрывает: range на 2-м поле PK
        // эффективен только когда 1-е поле фиксировано)
        exec(client, "CREATE INDEX IF NOT EXISTS IDX_DB_OPDAY ON DAYBALANCES (CCOPERATIONDAY)");
        exec(client, "DELETE FROM DAYBALANCES");
        for (int r = 1; r <= registers; r++) {
            String regId = String.format("R%03d", r);
            for (int d = 0; d < days; d++) {
                LocalDate operDay = today.minusDays(d);
                Date sqlDay = Date.valueOf(operDay);
                BigDecimal start = new BigDecimal((1000 + r * 100) + ".00");
                BigDecimal dtSum = new BigDecimal((100 + d * 10) + ".00");
                BigDecimal ktSum = new BigDecimal((50 + d * 5) + ".00");
                BigDecimal finish = start.add(ktSum).subtract(dtSum);
                execArgs(client,
                        "INSERT INTO DAYBALANCES (REGISTER, CCOPERATIONDAY, CCSTARTSUM, CCSTARTSUMNAT, " +
                                " CCDTSUM, CCDTSUMNAT, CCKTSUM, CCKTSUMNAT, " +
                                " CCDTCOUNT, CCKTCOUNT, CCFINISHSUM, CCFINISHSUMNAT) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        regId, sqlDay, start, start, dtSum, dtSum, ktSum, ktSum,
                        BigDecimal.ONE, BigDecimal.ONE, finish, finish);
                s.dayBalances++;
            }
        }

        return s;
    }

    private void exec(IgniteClient c, String sql) {
        try (var cur = c.query(new SqlFieldsQuery(sql))) { cur.iterator().forEachRemaining(r -> {}); }
    }
    private void execArgs(IgniteClient c, String sql, Object... args) {
        try (var cur = c.query(new SqlFieldsQuery(sql).setArgs(args))) {
            cur.iterator().forEachRemaining(r -> {});
        }
    }
}
