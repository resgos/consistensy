package ru.sbrf.pprb.stmnt.consistency.lib;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.client.IgniteClient;
import org.springframework.stereotype.Component;
import ru.sbrf.pprb.stmnt.consistency.config.ConsistencyProperties;
import ru.sbrf.pprb.stmnt.consistency.integration.ignite.IgniteClientFactory;
import ru.sbrf.pprb.stmnt.consistency.lib.events.DomainEvents;
import ru.sbrf.pprb.stmnt.consistency.lib.events.EventPublisher;
import ru.sbrf.stmnt.ignite.service.DayBalancesAdminService;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Fan-out admin operations to one or more Ignite clusters via thin-client
 * service proxy.
 *
 * Each cluster is called in parallel; results are collected per cluster.
 * If a cluster is unreachable, its result entry has ok=false and the error message.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AdminFanOut {

    private final IgniteClientFactory clientFactory;
    private final ConsistencyProperties props;
    private final ErrorRegistry errors;
    private final EventPublisher events;

    private static final String SERVICE_NAME = "DayBalancesAdmin";

    private final ExecutorService pool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "admin-fanout");
        t.setDaemon(true);
        return t;
    });

    /** Run init across selected clusters. */
    public Map<String, Map<String, Object>> init(String registerId,
                                                  String fromDate,
                                                  String toDate,
                                                  BigDecimal opening,
                                                  BigDecimal openingNat,
                                                  List<String> clusterIds) {
        return fanOut(clusterIds, "init", registerId, admin ->
                admin.initRegister(registerId, fromDate, toDate, opening, openingNat));
    }

    /** Run cleanup across selected clusters. */
    public Map<String, Map<String, Object>> cleanup(String beforeDate,
                                                     String registerFilter,
                                                     List<String> clusterIds) {
        return fanOut(clusterIds, "cleanup", registerFilter, admin ->
                admin.cleanupBalances(beforeDate, registerFilter));
    }

    /** Run regular recalc across selected clusters. */
    public Map<String, Map<String, Object>> recalc(String registerId,
                                                    String fromDate,
                                                    String toDate,
                                                    List<String> clusterIds) {
        return fanOut(clusterIds, "recalc", registerId, admin -> {
            admin.recalcRegisterRange(registerId, fromDate, toDate);
            return "ok";
        });
    }

    // -------- internals -------------------------------------------------------

    @FunctionalInterface
    private interface AdminCall {
        Object apply(DayBalancesAdminService admin) throws Exception;
    }

    private Map<String, Map<String, Object>> fanOut(List<String> clusterIds,
                                                     String opName,
                                                     String registerId,
                                                     AdminCall call) {
        List<String> targets = (clusterIds == null || clusterIds.isEmpty())
                ? props.getClusters().stream()
                    .map(ConsistencyProperties.Cluster::getId).toList()
                : clusterIds;

        Map<String, CompletableFuture<Map<String, Object>>> futures = new LinkedHashMap<>();
        for (String cid : targets) {
            futures.put(cid, CompletableFuture.supplyAsync(
                    () -> invokeOne(cid, opName, call), pool));
        }
        Map<String, Map<String, Object>> result = new LinkedHashMap<>();
        for (var e : futures.entrySet()) result.put(e.getKey(), e.getValue().join());

        // Финальный domain-event — fan-out завершён по всем кластерам.
        try {
            events.publish(new DomainEvents.AdminOperationCompleted(
                    opName, registerId, targets, result, Instant.now()));
        } catch (Exception ex) {
            log.warn("AdminFanOut: event publish failed (op={}): {}", opName, ex.toString());
        }
        return result;
    }

    private Map<String, Object> invokeOne(String clusterId, String opName, AdminCall call) {
        IgniteClient client = clientFactory.get(clusterId);
        if (client == null) {
            errors.record("admin_fanout", "WARN", "CLUSTER_DOWN",
                    "Cluster client not initialized for admin op " + opName,
                    Map.of("cluster", clusterId, "op", opName),
                    null, clusterId, null);
            return Map.of("ok", false, "error", "cluster not connected");
        }
        try {
            DayBalancesAdminService admin = client.services()
                    .serviceProxy(SERVICE_NAME, DayBalancesAdminService.class);
            Object res = call.apply(admin);
            log.info("Admin {} cluster={} ok result={}", opName, clusterId, res);
            return Map.of("ok", true, "result", String.valueOf(res));
        } catch (Exception e) {
            log.error("Admin {} cluster={} failed: {}", opName, clusterId, e.toString(), e);
            errors.error("admin_fanout", opName.toUpperCase() + "_FAILED",
                    "Admin " + opName + " failed on cluster " + clusterId, e,
                    Map.of("cluster", clusterId, "op", opName));
            return Map.of("ok", false, "error", e.toString());
        }
    }
}
