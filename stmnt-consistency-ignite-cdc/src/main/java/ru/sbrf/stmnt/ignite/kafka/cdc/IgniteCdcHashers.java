package ru.sbrf.stmnt.ignite.kafka.cdc;

import org.apache.ignite.binary.BinaryObject;
import ru.sbrf.pprb.stmnt.consistency.hashers.CdcEventHasher;
import ru.sbrf.pprb.stmnt.consistency.hashers.HashUtil;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Реализации CdcEventHasher для 8 интересующих кешей.
 *
 * Контракт — БУКВА В БУКВУ совпадает с pull-mode Hashers.java в
 * consistency-service: те же поля, тот же порядок, та же канонизация
 * (через shared HashUtil из stmnt-consistency-hashers).
 *
 * Поля читаем через BinaryObject.field("...") — не зависим от DTO-classpath,
 * не нужен deserialize. Имена полей = Java field names DTO-классов.
 *
 * NB: INCOME_SALDO здесь нет — это Postgres-таблица, не Ignite-cache.
 * Для неё CDC-канал не существует; используется pull-mode при необходимости.
 */
public final class IgniteCdcHashers {

    private IgniteCdcHashers() {}

    public static Map<String, CdcEventHasher> all() {
        Map<String, CdcEventHasher> m = new LinkedHashMap<>();
        m.put("REGISTER",        new RegisterHasher());
        m.put("TURN_DOC_CUR",    new TurnDocCurHasher());
        m.put("CLIENT",          new ClientHasher());
        m.put("CURRENCY",        new CurrencyHasher());
        m.put("DAY_BALANCES",    new DayBalancesHasher());
        m.put("DIVISION",        new DivisionHasher());
        m.put("CB_RATE",         new CbRateHasher());
        m.put("CASH_SYMBOL_DOC", new CashSymbolDocHasher());
        return m;
    }

    // ---- helpers ----------------------------------------------------------

    private static Object f(Object value, String field) {
        if (value instanceof BinaryObject) return ((BinaryObject) value).field(field);
        // fallback на reflection — на случай если cache не в keepBinary режиме
        try {
            String getter = "get" + Character.toUpperCase(field.charAt(0)) + field.substring(1);
            return value.getClass().getMethod(getter).invoke(value);
        } catch (Exception e) {
            return null;
        }
    }

    private static String fs(Object v, String field) {
        return HashUtil.asString(f(v, field));
    }

    private static Object keyField(Object key, String field) {
        if (key instanceof BinaryObject) return ((BinaryObject) key).field(field);
        try {
            String getter = "get" + Character.toUpperCase(field.charAt(0)) + field.substring(1);
            return key.getClass().getMethod(getter).invoke(key);
        } catch (Exception e) {
            return null;
        }
    }

    // ---- REGISTER --------------------------------------------------------
    static final class RegisterHasher implements CdcEventHasher {
        @Override public String cacheName() { return "REGISTER"; }
        @Override public String businessKeyOf(Object key, Object v) {
            // RegisterObjectId.objectId = бизнес-id регистра (ccRegisterId).
            // Берём прямо из ключа — стабильно между кластерами по построению.
            String objectId = HashUtil.asString(keyField(key, "objectId"));
            if (objectId != null) return objectId;
            return HashUtil.asString(f(v, "ccRegisterId"));
        }
        @Override public String hashOf(Object key, Object v) {
            return HashUtil.md5Of(
                    fs(v, "ccRegisterId"),
                    HashUtil.asEpochMillis(f(v, "ccRqTm")),
                    HashUtil.asEpochMillis(f(v, "ccOpenDate")),
                    HashUtil.asEpochMillis(f(v, "ccCloseDate")),
                    fs(v, "currency"),
                    HashUtil.asEpochMillis(f(v, "ccBalanceRecalcDate")));
        }
    }

    // ---- TURN_DOC_CUR -----------------------------------------------------
    static final class TurnDocCurHasher implements CdcEventHasher {
        @Override public String cacheName() { return "TURN_DOC_CUR"; }
        @Override public String businessKeyOf(Object key, Object v) {
            // TurnDocCurAffinityKey.of(register, ccIdEKS) уже строит
            // objectId = "register:ccIdEKS" — это и есть canonical business key,
            // стабильный между кластерами по построению (одни и те же register+ccIdEKS
            // дадут один и тот же objectId на c1/c2/c3).
            // Берём прямо из affinity-key — это надёжнее чем собирать из value
            // (value может приходить с разными ccRqUId на ретраях, и т.п.).
            String objectId = HashUtil.asString(keyField(key, "objectId"));
            if (objectId != null) return objectId;
            // Fallback: reconstruct из value-полей (если key не AffinityKey)
            return HashUtil.asString(f(v, "register")) + ":" +
                    HashUtil.asString(f(v, "ccIdEKS"));
        }
        @Override public String hashOf(Object key, Object v) {
            Object t = f(v, "ccTypeOper");
            int typeOper = (t == null) ? -1 : HashUtil.asBigDecimal(t).intValue();
            if (typeOper == 50) {
                return HashUtil.md5Of(
                        "T50",
                        HashUtil.asBigDecimal(f(v, "ccStartSum")),
                        HashUtil.asBigDecimal(f(v, "ccStartSumNAT")),
                        fs(v, "ccIdEKS"),
                        fs(v, "register"));
            }
            return HashUtil.md5Of(
                    fs(v, "ccIdEKS"),
                    fs(v, "register"),
                    HashUtil.asBigDecimal(f(v, "ccSum")),
                    fs(v, "ccRqUId"));
        }
    }

    // ---- CLIENT -----------------------------------------------------------
    static final class ClientHasher implements CdcEventHasher {
        @Override public String cacheName() { return "CLIENT"; }
        @Override public String businessKeyOf(Object key, Object v) {
            return HashUtil.asString(f(v, "ccEPK"));
        }
        @Override public String hashOf(Object key, Object v) {
            return HashUtil.md5Of(
                    fs(v, "ccEPK"),
                    HashUtil.asEpochMillis(f(v, "ccRqTm")),
                    fs(v, "ccName"),
                    fs(v, "ccINN"));
        }
    }

    // ---- CURRENCY ---------------------------------------------------------
    static final class CurrencyHasher implements CdcEventHasher {
        @Override public String cacheName() { return "CURRENCY"; }
        @Override public String businessKeyOf(Object key, Object v) {
            return HashUtil.asString(f(v, "ccEKSCode"));
        }
        @Override public String hashOf(Object key, Object v) {
            return HashUtil.md5Of(
                    fs(v, "ccEKSCode"),
                    fs(v, "ccEXTCode"),
                    fs(v, "ccName"),
                    fs(v, "ccAlphaCode"),
                    fs(v, "ccAlphaNumCode"));
        }
    }

    // ---- DAY_BALANCES -----------------------------------------------------
    static final class DayBalancesHasher implements CdcEventHasher {
        @Override public String cacheName() { return "DAY_BALANCES"; }
        @Override public String businessKeyOf(Object key, Object v) {
            // Production: DayBalancesAffinityKey.of(register, ccBalanceDate)
            //   → key.objectId = "register:ccBalanceDate" — каноническая форма.
            String objectId = HashUtil.asString(keyField(key, "objectId"));
            if (objectId != null) return objectId;

            // Smoke (SQL DDL): key — synthesized PK-class с полями REGISTER /
            // CCOPERATIONDAY (uppercase, case-insensitive matching). value — то же.
            // Собираем "register:date" вручную, перебирая возможные имена полей.
            String reg = (String) firstNonNull(
                    HashUtil.asString(keyField(key, "register")),
                    HashUtil.asString(f(v, "register")));
            Object date = firstNonNull(
                    keyField(key, "ccBalanceDate"),
                    keyField(key, "ccOperationDay"),     // production-альтернатива
                    f(v, "ccBalanceDate"),
                    f(v, "ccOperationDay"),
                    keyField(key, "CCOPERATIONDAY"),     // smoke uppercase explicit
                    f(v, "CCOPERATIONDAY"));
            return reg + ":" + HashUtil.asString(date);
        }
        @Override public String hashOf(Object key, Object v) {
            // Tolerance 0.01 — round2 для денег. Та же формула что в pull-mode hashers.
            Object dateField = firstNonNull(
                    f(v, "ccBalanceDate"),
                    f(v, "ccOperationDay"),
                    f(v, "CCOPERATIONDAY"));
            return HashUtil.md5Of(
                    fs(v, "register"),
                    HashUtil.asEpochMillis(dateField),
                    HashUtil.round2(f(v, "ccStartSum")),
                    HashUtil.round2(f(v, "ccStartSumNAT")),
                    HashUtil.round2(f(v, "ccDtSum")),
                    HashUtil.round2(f(v, "ccDtSumNAT")),
                    HashUtil.round2(f(v, "ccKtSum")),
                    HashUtil.round2(f(v, "ccKtSumNAT")),
                    HashUtil.asBigDecimal(f(v, "ccDtCount")),
                    HashUtil.asBigDecimal(f(v, "ccKtCount")),
                    HashUtil.round2(f(v, "ccFinishSum")),
                    HashUtil.round2(f(v, "ccFinishSumNAT")));
        }
    }

    private static Object firstNonNull(Object... values) {
        for (Object v : values) if (v != null) return v;
        return null;
    }

    // ---- DIVISION ---------------------------------------------------------
    static final class DivisionHasher implements CdcEventHasher {
        @Override public String cacheName() { return "DIVISION"; }
        @Override public String businessKeyOf(Object key, Object v) {
            return HashUtil.asString(f(v, "ccDivisionId"));
        }
        @Override public String hashOf(Object key, Object v) {
            return HashUtil.md5Of(
                    fs(v, "ccDivisionId"),
                    fs(v, "ccTBCode"),
                    fs(v, "ccOSBCode"),
                    fs(v, "ccFullDivCode"),
                    fs(v, "ccFullName"),
                    fs(v, "ccBIC"),
                    fs(v, "ccINN"),
                    fs(v, "ccKPP"),
                    HashUtil.asEpochMillis(f(v, "ccRqTm")));
        }
    }

    // ---- CB_RATE ----------------------------------------------------------
    static final class CbRateHasher implements CdcEventHasher {
        @Override public String cacheName() { return "CB_RATE"; }
        @Override public String businessKeyOf(Object key, Object v) {
            // CB_RATE.code:date — стабильный business-key
            return HashUtil.asString(f(v, "ccCode")) + ":" +
                    HashUtil.asString(f(v, "ccDate"));
        }
        @Override public String hashOf(Object key, Object v) {
            return HashUtil.md5Of(
                    fs(v, "ccCode"),
                    HashUtil.asEpochMillis(f(v, "ccDate")),
                    HashUtil.asBigDecimal(f(v, "ccRate")),
                    HashUtil.asBigDecimal(f(v, "ccLotSize")));
        }
    }

    // ---- CASH_SYMBOL_DOC -------------------------------------------------
    static final class CashSymbolDocHasher implements CdcEventHasher {
        @Override public String cacheName() { return "CASH_SYMBOL_DOC"; }
        @Override public String businessKeyOf(Object key, Object v) {
            // CashSymbolDocAffinityKey.of(register, ccideks, number) строит
            // objectId = "register:ccideks:number". Используем как BK.
            String objectId = HashUtil.asString(keyField(key, "objectId"));
            if (objectId != null) return objectId;
            // Fallback (если key — не AffinityKey): собрать вручную.
            return HashUtil.asString(f(v, "register")) + ":" +
                    HashUtil.asString(f(v, "turnDocId")) + ":" +
                    HashUtil.asString(f(v, "number"));
        }
        @Override public String hashOf(Object key, Object v) {
            return HashUtil.md5Of(
                    fs(v, "register"),
                    fs(v, "number"),
                    fs(v, "turnDocId"),
                    fs(v, "ccCode"),
                    fs(v, "ccSource"),
                    HashUtil.asBigDecimal(f(v, "ccSum")),
                    fs(v, "ccPriority"));
        }
    }
}
