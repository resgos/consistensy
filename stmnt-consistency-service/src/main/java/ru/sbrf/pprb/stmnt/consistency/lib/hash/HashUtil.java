package ru.sbrf.pprb.stmnt.consistency.lib.hash;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HexFormat;

/**
 * Field normalization + MD5 helpers.
 *
 * Rules (per the spec):
 *  - null  -> "NULL"
 *  - BigDecimal -> fixed-scale 6 (HALF_UP), to avoid scale-induced false negatives
 *  - Date / Timestamp / LocalDate / LocalDateTime -> Unix epoch millis (deterministic across TZ)
 *  - Other types -> toString()
 *  - Fields joined with "|"
 */
public final class HashUtil {

    private static final HexFormat HEX = HexFormat.of();
    public static final String NULL_MARKER = "NULL";
    private static final int BIGDECIMAL_SCALE = 6;

    private HashUtil() {}

    /** Build the canonical string from fields (in given order), separated by "|". */
    public static String join(Object... fields) {
        StringBuilder sb = new StringBuilder(64);
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) sb.append('|');
            sb.append(stringify(fields[i]));
        }
        return sb.toString();
    }

    /** MD5 hex of the canonical string. */
    public static String md5(String canonical) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HEX.formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 not available", e);
        }
    }

    /** Convenience: md5(join(fields)). */
    public static String md5Of(Object... fields) {
        return md5(join(fields));
    }

    /** Public for testing — exposed so test code can verify normalization rules directly. */
    public static String stringify(Object v) {
        if (v == null) return NULL_MARKER;
        if (v instanceof BigDecimal bd) {
            return bd.setScale(BIGDECIMAL_SCALE, RoundingMode.HALF_UP).toPlainString();
        }
        if (v instanceof java.util.Date d) {
            return Long.toString(d.getTime());
        }
        if (v instanceof LocalDate ld) {
            return Long.toString(ld.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());
        }
        if (v instanceof LocalDateTime ldt) {
            return Long.toString(ldt.toInstant(ZoneOffset.UTC).toEpochMilli());
        }
        return v.toString();
    }

    /** Conversion helpers used by hashers when reading List<?> rows. */
    public static Object asBigDecimal(Object v) {
        if (v == null) return null;
        if (v instanceof BigDecimal bd) return bd;
        if (v instanceof Number n) return new BigDecimal(n.toString());
        return new BigDecimal(v.toString());
    }

    public static Object asEpochMillis(Object v) {
        if (v == null) return null;
        if (v instanceof java.util.Date d) return d.getTime();
        if (v instanceof LocalDate ld) return ld.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        if (v instanceof LocalDateTime ldt) return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
        if (v instanceof Number n) return n.longValue();
        return v.toString();
    }

    public static String asString(Object v) {
        return v == null ? null : v.toString();
    }
}
