package ru.sbrf.pprb.stmnt.consistency.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Bound to "consistency.*" properties in application.yml.
 */
@Data
@Component
@ConfigurationProperties(prefix = "consistency")
public class ConsistencyProperties {

    /** Endpoints of Ignite clusters to read from. */
    private List<Cluster> clusters = new ArrayList<>();

    /** Caches to check. Defaults to all 9 supported caches if not specified. */
    private List<String> caches = List.of(
            "REGISTER", "TURN_DOC_CUR", "CLIENT", "CURRENCY",
            "DAY_BALANCES", "DIVISION", "INCOME_SALDO", "CB_RATE", "CASH_SYMBOL_DOC");

    /**
     * Snapshot lag — skip records modified within this window to avoid inflight
     * inconsistency from async cross-cluster propagation.
     */
    private Duration snapshotLag = Duration.ofMinutes(5);

    /**
     * Lookback window for high-volume caches (TURN_DOC_CUR, DAY_BALANCES) —
     * only records with operationDay within [today - turnLookbackDays, today] are checked.
     */
    private int turnLookbackDays = 3;

    /** Cron expression for scheduled runs (default: every hour at :05). */
    private String cron = "0 5 * * * *";

    /** Hash retention in days. */
    private int hashRetentionDays = 30;

    /**
     * Debug switch: when true, ClusterReader runs EXPLAIN before each hasher SQL and
     * logs the plan. Useful for verifying that index scans / reverse scans / co-located
     * joins are picked. Disable in production — adds extra round-trip per query.
     */
    private boolean debugExplainPlans = false;

    @Data
    public static class Cluster {
        /** Logical id, e.g. "cluster-1". */
        private String id;
        /** Thin-client addresses, e.g. ["ignite-1a:10800", "ignite-1b:10800"]. */
        private List<String> addresses = new ArrayList<>();
    }
}
