package ru.sbrf.pprb.stmnt.consistency.lib.hash;

import java.util.List;

/**
 * One hasher per cache. Reads rows from a SqlFieldsQuery cursor and produces
 *   businessKey -> hashHex
 * with normalized field stringification (see HashUtil).
 */
public interface HashCalculator {

    /** Logical cache name (e.g. "REGISTER"). */
    String cacheName();

    /**
     * SQL to run via thin client. Must return the projection {@link #rowToBusinessKey}
     * and {@link #rowToHash} expect.
     *
     * The placeholder ? in this SQL is filled with parameters from {@link #queryParams}.
     */
    String selectSql();

    /** Parameters for selectSql() placeholders (in order). May be empty. */
    default Object[] queryParams() {
        return new Object[0];
    }

    /** Extract business key from a row. */
    String rowToBusinessKey(List<?> row);

    /** Compute hash hex for a row. */
    String rowToHash(List<?> row);
}
