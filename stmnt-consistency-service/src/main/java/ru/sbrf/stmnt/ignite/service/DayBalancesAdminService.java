package ru.sbrf.stmnt.ignite.service;

import org.apache.ignite.services.Service;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Ignite ClusterSingleton service exposing administrative actions of
 * DayBalancesRecalcService to thin clients.
 *
 * Thin client usage:
 * <pre>
 *   try (IgniteClient client = Ignition.startClient(cfg)) {
 *       DayBalancesAdminService admin = client.services()
 *           .serviceProxy("DayBalancesAdmin", DayBalancesAdminService.class);
 *
 *       InitResult r = admin.initRegister("R001",
 *           LocalDate.of(2026, 1, 1), LocalDate.of(2026, 5, 24),
 *           new BigDecimal("1000.00"), null);
 *   }
 * </pre>
 *
 * NOTE: this interface must not extend Ignite types that the thin client
 * doesn't have on its classpath. Service interface is fine — IgniteClient
 * has it bundled.
 */
public interface DayBalancesAdminService extends Service {

    /**
     * Run an init pass for a single register, up to 180 days.
     *
     * @param registerId         id of register
     * @param fromDateIso        ISO "YYYY-MM-DD"; inclusive
     * @param toDateIso          ISO "YYYY-MM-DD"; inclusive, capped at yesterday
     * @param openingBalance     opening balance for fromDate (nullable → use existing chain)
     * @param openingBalanceNat  ruble equivalent (nullable → recompute through CB rate)
     */
    InitResult initRegister(String registerId,
                            String fromDateIso,
                            String toDateIso,
                            BigDecimal openingBalance,
                            BigDecimal openingBalanceNat);

    /**
     * Cleanup DayBalances and non-init type50 records older than beforeDate.
     *
     * @param beforeDateIso  ISO date; all records with CCOPERATIONDAY < this are removed
     * @param registerFilter null = cleanup all registers; non-null = only specified register
     */
    CleanupResult cleanupBalances(String beforeDateIso, String registerFilter);

    /**
     * Trigger a regular recalc for a single register over a date range.
     * Convenience for ops — calls recalcRegisterRange.
     */
    void recalcRegisterRange(String registerId, String fromDateIso, String toDateIso);

    /**
     * Trigger one pass of daily cleanup right now (instead of waiting for 04:00 cron).
     * Operation is no-op unless system property daybalances.cleanup.retention-days > 0.
     */
    void dailyCleanupNow();

    /**
     * Serializable DTOs — must not reference Ignite-server-only classes, so
     * we keep them inside the interface for thin-client compatibility.
     */
    final class InitResult implements Serializable {
        private static final long serialVersionUID = 1L;
        public String  registerId;
        public String  fromDate;
        public String  toDate;
        public int     totalDays;
        public int     batchesProcessed;
        public boolean ok;
        public String  error;
        @Override public String toString() {
            return "InitResult{register=" + registerId + " from=" + fromDate +
                    " to=" + toDate + " days=" + totalDays + " batches=" + batchesProcessed +
                    " ok=" + ok + (error != null ? " error=" + error : "") + "}";
        }
    }

    final class CleanupResult implements Serializable {
        private static final long serialVersionUID = 1L;
        public String  beforeDate;
        public String  registerFilter;
        public long    deletedDayBalances;
        public long    deletedType50;
        public boolean ok;
        public String  error;
        @Override public String toString() {
            return "CleanupResult{before=" + beforeDate + " regFilter=" + registerFilter +
                    " db=" + deletedDayBalances + " t50=" + deletedType50 +
                    " ok=" + ok + (error != null ? " error=" + error : "") + "}";
        }
    }
}
