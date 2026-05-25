package ru.sbrf.pprb.stmnt.consistency.lib.hash;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.sbrf.pprb.stmnt.consistency.config.ConsistencyProperties;

import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static ru.sbrf.pprb.stmnt.consistency.lib.hash.HashUtil.asBigDecimal;
import static ru.sbrf.pprb.stmnt.consistency.lib.hash.HashUtil.asEpochMillis;
import static ru.sbrf.pprb.stmnt.consistency.lib.hash.HashUtil.asString;
import static ru.sbrf.pprb.stmnt.consistency.lib.hash.HashUtil.md5Of;

/**
 * Per-cache hashers. Field order matches the spec exactly; do not reorder
 * without updating consumers (any change here invalidates historical hashes).
 *
 * EnrichDirectory is intentionally NOT implemented — per the spec it requires
 * a RQUID/version field to be added to the DTO first.
 */
@Component
@RequiredArgsConstructor
public final class Hashers {

    private final ConsistencyProperties props;

    /** Returns the full registry: cacheName -> HashCalculator. */
    public Map<String, HashCalculator> registry() {
        Map<String, HashCalculator> m = new LinkedHashMap<>();
        m.put("REGISTER", new RegisterHasher());
        m.put("TURN_DOC_CUR", new TurnDocCurHasher(props.getTurnLookbackDays()));
        m.put("CLIENT", new ClientHasher());
        m.put("CURRENCY", new CurrencyHasher());
        m.put("DAY_BALANCES", new DayBalancesHasher(props.getTurnLookbackDays()));
        m.put("DIVISION", new DivisionHasher());
        m.put("INCOME_SALDO", new IncomeSaldoHasher());
        m.put("CB_RATE", new CbRateHasher());
        m.put("CASH_SYMBOL_DOC", new CashSymbolDocHasher());
        return m;
    }

    // -------- REGISTER --------------------------------------------------------
    static final class RegisterHasher implements HashCalculator {
        @Override public String cacheName() { return "REGISTER"; }
        @Override public String selectSql() {
            return "SELECT OBJECTID, CCRQTM, CCOPENDATE, CCCLOSEDATE, CURRENCY, CCBALANCERECALCDATE " +
                    "FROM REGISTER.REGISTER";
        }
        @Override public String rowToBusinessKey(List<?> row) { return asString(row.get(0)); }
        @Override public String rowToHash(List<?> row) {
            // ccRegisterId|ccRqTm|ccOpenDate|ccCloseDate|currency|ccBalanceRecalcDate
            return md5Of(
                    asString(row.get(0)),
                    asEpochMillis(row.get(1)),
                    asEpochMillis(row.get(2)),
                    asEpochMillis(row.get(3)),
                    asString(row.get(4)),
                    asEpochMillis(row.get(5)));
        }
    }

    // -------- TURN_DOC_CUR ---------------------------------------------------
    /**
     * Two modes:
     *   typeOper = 50 : T50|ccStartSum|ccStartSumNat|ccIdEKS|ccRegisterId
     *   other         : ccIdEKS|ccRegisterId|ccSum|ccRqUId
     *
     * Scope is limited to the last N days of operationDay (turnLookbackDays).
     */
    @RequiredArgsConstructor
    static final class TurnDocCurHasher implements HashCalculator {
        private final int lookbackDays;
        @Override public String cacheName() { return "TURN_DOC_CUR"; }
        @Override public String selectSql() {
            return "SELECT OBJECTID, CCTYPEOPER, CCSTARTSUM, CCSTARTSUMNAT, " +
                    "       CCIDEKS, REGISTER, CCSUM, CCRQUID " +
                    "FROM TURN_DOC_CUR.TURNDOCCUR " +
                    "WHERE CCOPERATIONDAY >= ?";
        }
        @Override public Object[] queryParams() {
            return new Object[]{ java.sql.Date.valueOf(LocalDate.now().minusDays(lookbackDays)) };
        }
        @Override public String rowToBusinessKey(List<?> row) { return asString(row.get(0)); }
        @Override public String rowToHash(List<?> row) {
            Object typeOperObj = row.get(1);
            int typeOper = typeOperObj == null ? -1
                    : ((java.math.BigDecimal) asBigDecimal(typeOperObj)).intValueExact();
            if (typeOper == 50) {
                return md5Of(
                        "T50",
                        asBigDecimal(row.get(2)),
                        asBigDecimal(row.get(3)),
                        asString(row.get(4)),
                        asString(row.get(5)));
            }
            return md5Of(
                    asString(row.get(4)),
                    asString(row.get(5)),
                    asBigDecimal(row.get(6)),
                    asString(row.get(7)));
        }
    }

    // -------- CLIENT ---------------------------------------------------------
    static final class ClientHasher implements HashCalculator {
        @Override public String cacheName() { return "CLIENT"; }
        @Override public String selectSql() {
            return "SELECT OBJECTID, CCEPK, CCRQTM, CCNAME, CCINN FROM CLIENT.CLIENT";
        }
        @Override public String rowToBusinessKey(List<?> row) { return asString(row.get(0)); }
        @Override public String rowToHash(List<?> row) {
            // ccEPK|ccRqTm|ccName|ccINN
            return md5Of(
                    asString(row.get(1)),
                    asEpochMillis(row.get(2)),
                    asString(row.get(3)),
                    asString(row.get(4)));
        }
    }

    // -------- CURRENCY -------------------------------------------------------
    static final class CurrencyHasher implements HashCalculator {
        @Override public String cacheName() { return "CURRENCY"; }
        @Override public String selectSql() {
            return "SELECT OBJECTID, CCEKSCODE, CCEXTCODE, CCNAME, CCALPHACODE, CCALPHANUMCODE " +
                    "FROM CURRENCY.CURRENCY";
        }
        @Override public String rowToBusinessKey(List<?> row) { return asString(row.get(0)); }
        @Override public String rowToHash(List<?> row) {
            // ccEKSCode|ccEXTCode|ccName|ccAlphaCode|ccAlphaNumCode
            return md5Of(
                    asString(row.get(1)),
                    asString(row.get(2)),
                    asString(row.get(3)),
                    asString(row.get(4)),
                    asString(row.get(5)));
        }
    }

    // -------- DAY_BALANCES ---------------------------------------------------
    /**
     * Format: REGISTER|CCBALANCEDATE|ccStartSum|ccStartSumNat|ccDtSum|ccDtSumNat|
     *         ccKtSum|ccKtSumNat|ccDtCount|ccKtCount|ccFinishSum|ccFinishSumNat
     *
     * Note: tolerance 0.01 is enforced at compare-time by rounding to 2 decimals
     * before hashing here (instead of the standard 6-decimal scale used elsewhere).
     */
    @RequiredArgsConstructor
    static final class DayBalancesHasher implements HashCalculator {
        private final int lookbackDays;
        @Override public String cacheName() { return "DAY_BALANCES"; }
        @Override public String selectSql() {
            return "SELECT REGISTER, CCOPERATIONDAY, " +
                    "       CCSTARTSUM, CCSTARTSUMNAT, CCDTSUM, CCDTSUMNAT, " +
                    "       CCKTSUM, CCKTSUMNAT, CCDTCOUNT, CCKTCOUNT, " +
                    "       CCFINISHSUM, CCFINISHSUMNAT " +
                    "FROM DAY_BALANCES.DAYBALANCES " +
                    "WHERE CCOPERATIONDAY >= ?";
        }
        @Override public Object[] queryParams() {
            return new Object[]{ java.sql.Date.valueOf(LocalDate.now().minusDays(lookbackDays)) };
        }
        @Override public String rowToBusinessKey(List<?> row) {
            return asString(row.get(0)) + ":" + asString(row.get(1));
        }
        @Override public String rowToHash(List<?> row) {
            // Tolerance 0.01 — pre-round all monetary values to 2 decimals before hashing.
            return md5Of(
                    asString(row.get(0)),                     // REGISTER
                    asEpochMillis(row.get(1)),                // CCOPERATIONDAY
                    round2(row.get(2)), round2(row.get(3)),   // start, startNat
                    round2(row.get(4)), round2(row.get(5)),   // dt, dtNat
                    round2(row.get(6)), round2(row.get(7)),   // kt, ktNat
                    asBigDecimal(row.get(8)), asBigDecimal(row.get(9)),  // counts
                    round2(row.get(10)), round2(row.get(11)));// finish, finishNat
        }
        private static java.math.BigDecimal round2(Object v) {
            java.math.BigDecimal bd = (java.math.BigDecimal) asBigDecimal(v);
            if (bd == null) return null;
            return bd.setScale(2, java.math.RoundingMode.HALF_UP);
        }
    }

    // -------- DIVISION -------------------------------------------------------
    static final class DivisionHasher implements HashCalculator {
        @Override public String cacheName() { return "DIVISION"; }
        @Override public String selectSql() {
            return "SELECT OBJECTID, CCTBCODE, CCOSBCODE, CCFULLDIVCODE, CCFULLNAME, " +
                    "       CCBIC, CCINN, CCKPP, CCRQTM " +
                    "FROM DIVISION.DIVISION";
        }
        @Override public String rowToBusinessKey(List<?> row) { return asString(row.get(0)); }
        @Override public String rowToHash(List<?> row) {
            // ccDivisionId|ccTBCode|ccOSBCode|ccFullDivCode|ccFullName|ccBIC|ccINN|ccKPP|ccRqTm
            return md5Of(
                    asString(row.get(0)),
                    asString(row.get(1)),
                    asString(row.get(2)),
                    asString(row.get(3)),
                    asString(row.get(4)),
                    asString(row.get(5)),
                    asString(row.get(6)),
                    asString(row.get(7)),
                    asEpochMillis(row.get(8)));
        }
    }

    // -------- INCOME_SALDO ---------------------------------------------------
    static final class IncomeSaldoHasher implements HashCalculator {
        @Override public String cacheName() { return "INCOME_SALDO"; }
        @Override public String selectSql() {
            return "SELECT OBJECTID, REGISTER, CCDATE, CCSYSTEMID, " +
                    "       CCSTARTSUM, CCSTARTSUMNAT, CCVALIDSALDO " +
                    "FROM INCOME_SALDO.INCOMESALDO";
        }
        @Override public String rowToBusinessKey(List<?> row) { return asString(row.get(0)); }
        @Override public String rowToHash(List<?> row) {
            // objectId|register|ccDate|ccSystemId|ccStartSum|ccStartSumNAT|ccValidSaldo
            return md5Of(
                    asString(row.get(0)),
                    asString(row.get(1)),
                    asEpochMillis(row.get(2)),
                    asString(row.get(3)),
                    asBigDecimal(row.get(4)),
                    asBigDecimal(row.get(5)),
                    asString(row.get(6)));
        }
    }

    // -------- CB_RATE --------------------------------------------------------
    static final class CbRateHasher implements HashCalculator {
        @Override public String cacheName() { return "CB_RATE"; }
        @Override public String selectSql() {
            return "SELECT OBJECTID, CCCODE, CCDATE, CCRATE, CCLOTSIZE FROM CB_RATE.CBRATE";
        }
        @Override public String rowToBusinessKey(List<?> row) {
            // OBJECTID is excluded from the hash but used as business key for lookup.
            return asString(row.get(0));
        }
        @Override public String rowToHash(List<?> row) {
            // CCCODE|CCDATE|CCRATE|CCLOTSIZE
            return md5Of(
                    asString(row.get(1)),
                    asEpochMillis(row.get(2)),
                    asBigDecimal(row.get(3)),
                    asBigDecimal(row.get(4)));
        }
    }

    // -------- CASH_SYMBOL_DOC ------------------------------------------------
    static final class CashSymbolDocHasher implements HashCalculator {
        @Override public String cacheName() { return "CASH_SYMBOL_DOC"; }
        @Override public String selectSql() {
            return "SELECT OBJECTID, REGISTER, NUMBER, TURNDOCID, CCCODE, CCSOURCE, CCSUM, CCPRIORITY " +
                    "FROM CASH_SYMBOL_DOC.CASHSYMBOLDOC";
        }
        @Override public String rowToBusinessKey(List<?> row) { return asString(row.get(0)); }
        @Override public String rowToHash(List<?> row) {
            // REGISTER|NUMBER|TURNDOCID|CCCODE|CCSOURCE|CCSUM|CCPRIORITY
            return md5Of(
                    asString(row.get(1)),
                    asString(row.get(2)),
                    asString(row.get(3)),
                    asString(row.get(4)),
                    asString(row.get(5)),
                    asBigDecimal(row.get(6)),
                    asString(row.get(7)));
        }
    }
}
