package ru.sbrf.pprb.stmnt.consistency.rpc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.springframework.web.bind.annotation.*;
import ru.sbrf.pprb.stmnt.consistency.config.ConsistencyProperties;
import ru.sbrf.pprb.stmnt.consistency.integration.ignite.IgniteClientFactory;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Smoke-test эндпоинты для CDC через прямой cache.put (не через SQL DML).
 *
 * SQL DML в Apache Ignite 2.16 через H2 engine не всегда триггерит
 * ContinuousQuery; cache.put() — триггерит гарантированно. Этот контроллер
 * пишет в кеши напрямую через thin-client → CDC publisher на сервере ловит
 * событие → Kafka.
 *
 * Используется для:
 *   - smoke-проверки end-to-end CDC канала
 *   - воспроизведения сценариев расхождений когда нужен 100%-надёжный trigger
 */
@Slf4j
@RestController
@RequestMapping("/api/debug/cdc")
@RequiredArgsConstructor
public class CdcTestController {

    private final ConsistencyProperties props;
    private final IgniteClientFactory clientFactory;

    /**
     * POST /api/debug/cdc/put-register
     *   {clusterId, objectId, currency, ccRegisterId(optional)}
     * Кладёт одну запись в REGISTER cache через прямой putAll.
     */
    @PostMapping("/put-register")
    public Map<String, Object> putRegister(@RequestBody Map<String, Object> body) {
        String clusterId = (String) body.get("clusterId");
        String objectId  = (String) body.getOrDefault("objectId", "R001");
        String currency  = (String) body.getOrDefault("currency", "RUB");
        String ccReg     = (String) body.getOrDefault("ccRegisterId", objectId);
        // Фиксированный ccRqTm чтобы scenario с identical-значениями давал одинаковый hash на всех кластерах.
        // Можно override через body.ccRqTmMs для time-divergence сценариев.
        long ccRqTmMs = body.get("ccRqTmMs") instanceof Number
                ? ((Number) body.get("ccRqTmMs")).longValue()
                : 1700000000000L;

        IgniteClient c = clientFactory.get(clusterId);
        if (c == null) return Map.of("ok", false, "error", "cluster not connected: " + clusterId);

        ClientCache<Object, Object> cache = c.cache("REGISTER").withKeepBinary();
        BinaryObjectBuilder b = c.binary().builder("Register")
                .setField("objectId",            objectId)
                .setField("ccRegisterId",        ccReg)
                .setField("ccRqTm",              new Timestamp(ccRqTmMs))
                .setField("ccOpenDate",          Date.valueOf("2023-01-01"))
                .setField("currency",            currency)
                .setField("ccBalanceRecalcDate", Date.valueOf("2026-05-24"));
        cache.put(objectId, b.build());

        return Map.of("ok", true, "cluster", clusterId, "objectId", objectId,
                "currency", currency, "at", Instant.now().toString());
    }

    /**
     * POST /api/debug/cdc/scenario1-currency
     * Высокоуровневый сценарий: пишет R001 во все 3 кластера, но на cluster-1
     * с CURRENCY=EUR → REGISTER mismatch.
     */
    @PostMapping("/scenario1-currency")
    public Map<String, Object> scenario1Currency() {
        Map<String, Object> r = new LinkedHashMap<>();
        for (var c : props.getClusters()) {
            String currency = "cluster-1".equals(c.getId()) ? "EUR" : "RUB";
            Map<String, Object> req = new HashMap<>();
            req.put("clusterId", c.getId());
            req.put("objectId", "R001");
            req.put("currency", currency);
            r.put(c.getId(), putRegister(req));
        }
        return Map.of("scenario", "scenario1-currency",
                "expectedMismatch", "REGISTER:R001",
                "perCluster", r);
    }

    /**
     * POST /api/debug/cdc/scenario2-missing
     * R002 пишется на cluster-1 и cluster-3, но не на cluster-2 → MISSING.
     */
    @PostMapping("/scenario2-missing")
    public Map<String, Object> scenario2Missing() {
        Map<String, Object> r = new LinkedHashMap<>();
        for (var c : props.getClusters()) {
            if ("cluster-2".equals(c.getId())) {
                r.put(c.getId(), Map.of("skipped", true));
                continue;
            }
            Map<String, Object> req = new HashMap<>();
            req.put("clusterId", c.getId());
            req.put("objectId", "R002");
            req.put("currency", "USD");
            r.put(c.getId(), putRegister(req));
        }
        return Map.of("scenario", "scenario2-missing",
                "expectedMismatch", "REGISTER:R002 (REMOVED on cluster-2)",
                "perCluster", r);
    }

    /**
     * POST /api/debug/cdc/scenario3-extra
     * R999 — только на cluster-3, на других нет → односторонний mismatch.
     */
    @PostMapping("/scenario3-extra")
    public Map<String, Object> scenario3Extra() {
        Map<String, Object> req = new HashMap<>();
        req.put("clusterId", "cluster-3");
        req.put("objectId", "R999");
        req.put("currency", "GBP");
        return Map.of("scenario", "scenario3-extra",
                "expectedMismatch", "REGISTER:R999 (only on cluster-3)",
                "result", putRegister(req));
    }

    /**
     * POST /api/debug/cdc/scenario4-identical
     * R100 одинаково на всех — sweep должен НЕ найти mismatch (negative case).
     */
    @PostMapping("/scenario4-identical")
    public Map<String, Object> scenario4Identical() {
        Map<String, Object> r = new LinkedHashMap<>();
        for (var c : props.getClusters()) {
            Map<String, Object> req = new HashMap<>();
            req.put("clusterId", c.getId());
            req.put("objectId", "R100");
            req.put("currency", "RUB");
            r.put(c.getId(), putRegister(req));
        }
        return Map.of("scenario", "scenario4-identical",
                "expectedMismatch", "NONE",
                "perCluster", r);
    }

    /**
     * POST /api/debug/cdc/scenario5-delete
     * Удаляет R001 на cluster-2 → REMOVED event → mismatch (cluster-1/3 hash vs cluster-2 REMOVED).
     */
    @PostMapping("/scenario5-delete")
    public Map<String, Object> scenario5Delete() {
        IgniteClient c2 = clientFactory.get("cluster-2");
        if (c2 == null) return Map.of("ok", false, "error", "cluster-2 not connected");
        boolean removed = c2.cache("REGISTER").remove("R001");
        return Map.of("scenario", "scenario5-delete",
                "expectedMismatch", "REGISTER:R001 (REMOVED on cluster-2)",
                "removed", removed);
    }
}
