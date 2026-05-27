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
import java.util.*;

/**
 * Воспроизводит баг recalc DayBalances при перемещении проводки на другую дату.
 *
 *   POST /api/repro/recalc-bug/setup     — создаёт схему и сценарий
 *   POST /api/repro/recalc-bug/check     — проверяет состояние DAY_BALANCES
 *   POST /api/repro/recalc-bug/move      — перемещает проводку (день+сумма)
 *   POST /api/repro/recalc-bug/recalc?mode=buggy|fixed  — запускает recalc
 *
 * Сценарий из bug-описания:
 *   Шаг 1. INSERT TURNDOCCUR: REG=BUG_R001, day=2022-02-16, CCDT=1 (кредит), CCSUM=555
 *   Шаг 2. Recalc → DAYBALANCES row для 2022-02-16 с CCKTSUM=555
 *   Шаг 3. UPDATE проводки: day=2022-02-17, CCSUM=111
 *   Шаг 4. CCBALANCERECALCDATE = 2022-02-16
 *   Шаг 5. Recalc от 2022-02-16
 *     buggy:  DAYBALANCES для 2022-02-16 остаётся с нулевыми оборотами
 *     fixed:  DAYBALANCES для 2022-02-16 удалён
 *     DAYBALANCES для 2022-02-17 содержит CCKTSUM=111
 */
@Slf4j
@RestController
@RequestMapping("/api/repro/recalc-bug")
@RequiredArgsConstructor
public class RecalcBugReproController {

    private final IgniteClientFactory clientFactory;

    private static final String REG = "BUG_R001";
    private static final Date D_16 = Date.valueOf("2022-02-16");
    private static final Date D_17 = Date.valueOf("2022-02-17");
    private static final BigDecimal SUM_BEFORE = new BigDecimal("555.00");
    private static final BigDecimal SUM_AFTER = new BigDecimal("111.00");

    @PostMapping("/setup")
    public Map<String, Object> setup(@RequestParam(defaultValue = "cluster-1") String clusterId) {
        IgniteClient c = clientFactory.get(clusterId);
        if (c == null) return Map.of("ok", false, "error", "cluster not connected");

        // Создаём минимально нужные таблицы (если ещё не было bulk-seed)
        exec(c, "CREATE TABLE IF NOT EXISTS REGISTER ("
                + "  OBJECTID VARCHAR PRIMARY KEY, CCRQTM TIMESTAMP, CCOPENDATE DATE, CCCLOSEDATE DATE,"
                + "  CURRENCY VARCHAR, CCBALANCERECALCDATE DATE"
                + ") WITH \"CACHE_NAME=REGISTER, VALUE_TYPE=Register\"");

        exec(c, "CREATE TABLE IF NOT EXISTS TURNDOCCUR ("
                + "  OBJECTID VARCHAR PRIMARY KEY, CCRQUID VARCHAR, CCIDEKS VARCHAR, REGISTER VARCHAR,"
                + "  CCSUM DECIMAL, CCSUMNAT DECIMAL, CCSTARTSUM DECIMAL, CCSTARTSUMNAT DECIMAL,"
                + "  CCDT VARCHAR, CCTYPEOPER DECIMAL, CCDATE DATE, CCOPERATIONDAY DATE,"
                + "  CCDTINN VARCHAR, CCKTINN VARCHAR, CCDTACC VARCHAR, CCKTACC VARCHAR,"
                + "  CCPURPOSE VARCHAR, CCNUM VARCHAR"
                + ") WITH \"CACHE_NAME=TURN_DOC_CUR, VALUE_TYPE=TurnDocCur\"");

        exec(c, "CREATE TABLE IF NOT EXISTS DAYBALANCES ("
                + "  REGISTER VARCHAR, CCOPERATIONDAY DATE, CCSTARTSUM DECIMAL, CCSTARTSUMNAT DECIMAL,"
                + "  CCDTSUM DECIMAL, CCDTSUMNAT DECIMAL, CCKTSUM DECIMAL, CCKTSUMNAT DECIMAL,"
                + "  CCDTCOUNT DECIMAL, CCKTCOUNT DECIMAL, CCFINISHSUM DECIMAL, CCFINISHSUMNAT DECIMAL,"
                + "  PRIMARY KEY (REGISTER, CCOPERATIONDAY)"
                + ") WITH \"CACHE_NAME=DAY_BALANCES, VALUE_TYPE=DayBalances\"");

        // Чистим артефакты предыдущих прогонов
        execArgs(c, "DELETE FROM REGISTER WHERE OBJECTID=?", REG);
        execArgs(c, "DELETE FROM TURNDOCCUR WHERE REGISTER=?", REG);
        execArgs(c, "DELETE FROM DAYBALANCES WHERE REGISTER=?", REG);

        // Регистр
        execArgs(c, "INSERT INTO REGISTER (OBJECTID, CCOPENDATE, CURRENCY, CCBALANCERECALCDATE) " +
                "VALUES (?, ?, ?, ?)", REG, Date.valueOf("2022-01-01"), "RUB", D_16);

        // Шаг 1: проводка на 2022-02-16, кредит 555.00
        execArgs(c, "INSERT INTO TURNDOCCUR " +
                        "(OBJECTID, REGISTER, CCSUM, CCSUMNAT, CCDT, CCTYPEOPER, CCDATE, CCOPERATIONDAY) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                "BUG_DOC_1", REG, SUM_BEFORE, SUM_BEFORE, "1", new BigDecimal(0), D_16, D_16);

        // Первичный recalc (создаём DAYBALANCES для 2022-02-16)
        recalcRange(c, REG, D_16, D_17, "fixed");

        return Map.of(
                "ok", true,
                "step", "setup_done",
                "state", checkState(c)
        );
    }

    @PostMapping("/move")
    public Map<String, Object> move(@RequestParam(defaultValue = "cluster-1") String clusterId) {
        IgniteClient c = clientFactory.get(clusterId);
        if (c == null) return Map.of("ok", false, "error", "cluster not connected");

        // Шаг 3: UPDATE проводки — переносим на 17-е и меняем сумму на 111
        execArgs(c, "UPDATE TURNDOCCUR SET CCOPERATIONDAY=?, CCDATE=?, CCSUM=?, CCSUMNAT=? " +
                "WHERE OBJECTID=?",
                D_17, D_17, SUM_AFTER, SUM_AFTER, "BUG_DOC_1");

        // Шаг 4: CCBALANCERECALCDATE = 2022-02-16 (recalc должен пересчитать с этой даты)
        execArgs(c, "UPDATE REGISTER SET CCBALANCERECALCDATE=? WHERE OBJECTID=?", D_16, REG);

        return Map.of("ok", true, "step", "moved_to_17_sum_111", "state", checkState(c));
    }

    @PostMapping("/recalc")
    public Map<String, Object> recalc(@RequestParam(defaultValue = "buggy") String mode,
                                      @RequestParam(defaultValue = "cluster-1") String clusterId) {
        IgniteClient c = clientFactory.get(clusterId);
        if (c == null) return Map.of("ok", false, "error", "cluster not connected");

        recalcRange(c, REG, D_16, D_17, mode);

        Map<String, Object> state = checkState(c);
        boolean pass = (boolean) state.get("dayBalance_16_deleted");

        return Map.of(
                "ok", true,
                "mode", mode,
                "expected", "mode=fixed → строка за 2022-02-16 удалена; mode=buggy → осталась",
                "state", state,
                "test_passed_with_fixed", "fixed".equals(mode) ? pass : !pass
        );
    }

    @PostMapping("/check")
    public Map<String, Object> check(@RequestParam(defaultValue = "cluster-1") String clusterId) {
        IgniteClient c = clientFactory.get(clusterId);
        if (c == null) return Map.of("ok", false, "error", "cluster not connected");
        return Map.of("ok", true, "state", checkState(c));
    }

    // =====================================================================
    // Recalc-эмулятор (mirror логики DayBalancesRecalcService.recalcRegisterRange)
    // =====================================================================

    /**
     * Эмулирует DayBalancesRecalcService.recalcRegisterRange().
     *
     * mode=buggy  — всегда PUT в DAYBALANCES (как было до фикса).
     * mode=fixed  — если за день нет проводок (dtCount=0 && ktCount=0), DELETE из DAYBALANCES.
     */
    private void recalcRange(IgniteClient c, String register, Date fromDate, Date toDate, String mode) {
        LocalDate cur = fromDate.toLocalDate();
        LocalDate to = toDate.toLocalDate();
        BigDecimal startSum = findStartSum(c, register, cur);

        while (!cur.isAfter(to)) {
            Date sqlDay = Date.valueOf(cur);
            // Считаем обороты за день
            List<List<?>> rows = exec(c,
                    "SELECT " +
                            "  COALESCE(SUM(CASE WHEN CCDT='1' THEN 0 ELSE CCSUM END), 0), " +
                            "  COALESCE(SUM(CASE WHEN CCDT='1' THEN CCSUM ELSE 0 END), 0), " +
                            "  COUNT(CASE WHEN CCDT='0' THEN 1 END), " +
                            "  COUNT(CASE WHEN CCDT='1' THEN 1 END) " +
                            "FROM TURNDOCCUR WHERE REGISTER=? AND CCOPERATIONDAY=? AND CCTYPEOPER!=50",
                    register, sqlDay);

            BigDecimal dtSum = toBD(rows.get(0).get(0));   // CCDT='0' = дебет (приход)
            BigDecimal ktSum = toBD(rows.get(0).get(1));   // CCDT='1' = кредит (расход)
            int dtCount = toInt(rows.get(0).get(2));
            int ktCount = toInt(rows.get(0).get(3));
            BigDecimal finishSum = startSum.add(dtSum).subtract(ktSum);

            if (dtCount == 0 && ktCount == 0) {
                if ("fixed".equals(mode)) {
                    // [BUGFIX] За день нет проводок — УДАЛЯЕМ запись (как в Текущей Выписке)
                    execArgs(c,
                            "DELETE FROM DAYBALANCES WHERE REGISTER=? AND CCOPERATIONDAY=?",
                            register, sqlDay);
                    log.info("[fixed] deleted DAYBALANCES for register={} day={}", register, sqlDay);
                } else {
                    // buggy mode — пишем строку с нулевыми оборотами
                    upsertDayBalance(c, register, sqlDay, startSum, BigDecimal.ZERO, BigDecimal.ZERO,
                            0, 0, finishSum);
                    log.info("[buggy] wrote zero-turn DAYBALANCES for register={} day={}", register, sqlDay);
                }
            } else {
                upsertDayBalance(c, register, sqlDay, startSum, dtSum, ktSum, dtCount, ktCount, finishSum);
            }

            startSum = finishSum;
            cur = cur.plusDays(1);
        }
    }

    private BigDecimal findStartSum(IgniteClient c, String register, LocalDate fromDate) {
        // Упрощённый findStartSum — берём finishSum предыдущего дня или 0
        Date prev = Date.valueOf(fromDate.minusDays(1));
        List<List<?>> rows = exec(c,
                "SELECT CCFINISHSUM FROM DAYBALANCES WHERE REGISTER=? AND CCOPERATIONDAY=? LIMIT 1",
                register, prev);
        return rows.isEmpty() ? BigDecimal.ZERO : toBD(rows.get(0).get(0));
    }

    private void upsertDayBalance(IgniteClient c, String register, Date day,
                                  BigDecimal startSum, BigDecimal dtSum, BigDecimal ktSum,
                                  int dtCount, int ktCount, BigDecimal finishSum) {
        execArgs(c, "DELETE FROM DAYBALANCES WHERE REGISTER=? AND CCOPERATIONDAY=?", register, day);
        execArgs(c, "INSERT INTO DAYBALANCES " +
                        "(REGISTER, CCOPERATIONDAY, CCSTARTSUM, CCSTARTSUMNAT, CCDTSUM, CCDTSUMNAT, " +
                        " CCKTSUM, CCKTSUMNAT, CCDTCOUNT, CCKTCOUNT, CCFINISHSUM, CCFINISHSUMNAT) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                register, day, startSum, startSum,
                dtSum, dtSum, ktSum, ktSum,
                new BigDecimal(dtCount), new BigDecimal(ktCount),
                finishSum, finishSum);
    }

    // =====================================================================
    // Проверка состояния
    // =====================================================================

    private Map<String, Object> checkState(IgniteClient c) {
        Map<String, Object> s = new LinkedHashMap<>();

        // TURNDOCCUR
        List<List<?>> td = exec(c,
                "SELECT OBJECTID, CCOPERATIONDAY, CCSUM, CCDT FROM TURNDOCCUR WHERE REGISTER=?", REG);
        s.put("turndoc_count", td.size());
        s.put("turndoc_rows", td);

        // DAYBALANCES
        List<List<?>> db = exec(c,
                "SELECT CCOPERATIONDAY, CCSTARTSUM, CCDTSUM, CCKTSUM, CCDTCOUNT, CCKTCOUNT, CCFINISHSUM " +
                        "FROM DAYBALANCES WHERE REGISTER=? ORDER BY CCOPERATIONDAY", REG);
        s.put("daybalances_count", db.size());
        s.put("daybalances_rows", db);

        // Конкретные проверки для bug-описания
        boolean has16 = db.stream().anyMatch(r -> D_16.toLocalDate().equals(toLocalDate(r.get(0))));
        boolean has17 = db.stream().anyMatch(r -> D_17.toLocalDate().equals(toLocalDate(r.get(0))));
        s.put("dayBalance_16_exists", has16);
        s.put("dayBalance_16_deleted", !has16);
        s.put("dayBalance_17_exists", has17);

        return s;
    }

    // =====================================================================

    private List<List<?>> exec(IgniteClient c, String sql, Object... args) {
        SqlFieldsQuery q = new SqlFieldsQuery(sql);
        if (args != null && args.length > 0) q.setArgs(args);
        List<List<?>> result = new ArrayList<>();
        try (var cur = c.query(q)) {
            for (List<?> row : cur) result.add(row);
        }
        return result;
    }

    private void execArgs(IgniteClient c, String sql, Object... args) {
        exec(c, sql, args);
    }

    private static BigDecimal toBD(Object v) {
        if (v == null) return BigDecimal.ZERO;
        if (v instanceof BigDecimal) return (BigDecimal) v;
        return new BigDecimal(v.toString());
    }

    private static int toInt(Object v) {
        if (v == null) return 0;
        if (v instanceof Number) return ((Number) v).intValue();
        return Integer.parseInt(v.toString());
    }

    private static LocalDate toLocalDate(Object v) {
        if (v == null) return null;
        if (v instanceof LocalDate) return (LocalDate) v;
        if (v instanceof java.sql.Date) return ((java.sql.Date) v).toLocalDate();
        return null;
    }
}
