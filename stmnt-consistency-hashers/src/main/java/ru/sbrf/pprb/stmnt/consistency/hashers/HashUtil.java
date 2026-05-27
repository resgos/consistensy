package ru.sbrf.pprb.stmnt.consistency.hashers;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * SHARED. Канонизация полей + MD5. Используется одинаково обеими сторонами:
 *   - Ignite-side ContinuousQuery callback (считает hash из DTO)
 *   - consistency-service (если pull-mode включён) — из JDBC row
 *
 * Правила (часть контракта Kafka-сообщения; менять их — менять версию топика):
 *   null              -> "NULL"
 *   BigDecimal        -> scale=6 HALF_UP (исключает scale-induced false negatives)
 *   Date/Timestamp/LocalDate/LocalDateTime -> epoch millis UTC (TZ-detrimental)
 *   прочее            -> toString()
 *   join              -> поля через '|'
 *
 * NB: класс намеренно plain Java (без Lombok/Spring) — должен работать в Java 11
 * Ignite-lib и в Java 17 consistency-service.
 */
public final class HashUtil {

    public static final String NULL_MARKER = "NULL";
    private static final int BIGDECIMAL_SCALE = 6;
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private HashUtil() {}

    /** Канонический string из полей (в данном порядке), разделитель '|'. */
    public static String join(Object... fields) {
        StringBuilder sb = new StringBuilder(64);
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) sb.append('|');
            sb.append(stringify(fields[i]));
        }
        return sb.toString();
    }

    /** MD5 hex от canonical string. */
    public static String md5(String canonical) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(canonical.getBytes(StandardCharsets.UTF_8));
            return toHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 not available", e);
        }
    }

    /** Сокращение: md5(join(fields)). */
    public static String md5Of(Object... fields) {
        return md5(join(fields));
    }

    /** Public для тестов — проверять правила канонизации напрямую. */
    public static String stringify(Object v) {
        if (v == null) return NULL_MARKER;
        if (v instanceof BigDecimal) {
            return ((BigDecimal) v).setScale(BIGDECIMAL_SCALE, RoundingMode.HALF_UP).toPlainString();
        }
        if (v instanceof java.util.Date) {
            return Long.toString(((java.util.Date) v).getTime());
        }
        if (v instanceof LocalDate) {
            return Long.toString(((LocalDate) v).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());
        }
        if (v instanceof LocalDateTime) {
            return Long.toString(((LocalDateTime) v).toInstant(ZoneOffset.UTC).toEpochMilli());
        }
        return v.toString();
    }

    // ---- Helpers для приведения значений до подачи в md5Of ---------------------

    public static BigDecimal asBigDecimal(Object v) {
        if (v == null) return null;
        if (v instanceof BigDecimal) return (BigDecimal) v;
        if (v instanceof Number) return new BigDecimal(v.toString());
        return new BigDecimal(v.toString());
    }

    /** Получить epoch-millis из Date/LocalDate/LocalDateTime/Number — для join'а. */
    public static Object asEpochMillis(Object v) {
        if (v == null) return null;
        if (v instanceof java.util.Date) return ((java.util.Date) v).getTime();
        if (v instanceof LocalDate) return ((LocalDate) v).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        if (v instanceof LocalDateTime) return ((LocalDateTime) v).toInstant(ZoneOffset.UTC).toEpochMilli();
        if (v instanceof Number) return ((Number) v).longValue();
        return v.toString();
    }

    public static String asString(Object v) {
        return v == null ? null : v.toString();
    }

    /** Округлить BigDecimal до 2 знаков (используется DayBalances для tolerance=0.01). */
    public static BigDecimal round2(Object v) {
        BigDecimal bd = asBigDecimal(v);
        return bd == null ? null : bd.setScale(2, RoundingMode.HALF_UP);
    }

    private static String toHex(byte[] bytes) {
        char[] out = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int b = bytes[i] & 0xff;
            out[i * 2]     = HEX[b >>> 4];
            out[i * 2 + 1] = HEX[b & 0x0f];
        }
        return new String(out);
    }
}
