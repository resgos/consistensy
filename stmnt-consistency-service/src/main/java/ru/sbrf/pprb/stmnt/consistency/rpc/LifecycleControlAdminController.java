package ru.sbrf.pprb.stmnt.consistency.rpc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.client.IgniteClient;
import org.springframework.web.bind.annotation.*;
import ru.sbrf.pprb.stmnt.consistency.integration.ignite.IgniteClientFactory;

import java.sql.Timestamp;
import java.util.*;

/**
 * Admin REST endpoint для управления LIFECYCLE_CONTROL cache на Ignite-кластерах
 * через dropapp-сервис (consistency-service).
 *
 * <p>LIFECYCLE_CONTROL — Ignite-cache на стороне Ignite-кластера, в котором
 * LifecycleControlService регистрирует все LifecycleBean'ы (KafkaConsumer,
 * DayBalancesRecalc, KafkaCdcPublisher, и т.д.) с состоянием ENABLED/IS_LEADER
 * per-node. ContinuousQuery на каждой ноде слушает UPDATE-events и вызывает
 * {@code onEnabledChanged(boolean)} на соответствующем bean'е.
 *
 * <p>Этот контроллер — REST-fasade поверх SQL UPDATE на Ignite-cache.
 * Альтернативный путь — сразу из SQL-клиента подключенного к Ignite:
 * <pre>
 *   UPDATE LIFECYCLE_CONTROL SET ENABLED = false, UPDATED_AT = CURRENT_TIMESTAMP
 *    WHERE NODE_ID = 'node-02' AND BEAN_NAME = 'KafkaConsumer';
 * </pre>
 *
 * <p>REST-эндпоинты:
 * <ul>
 *   <li>{@code GET  /api/admin/lifecycle?clusterId=cluster-1} — список всех (node, bean, enabled, isLeader)</li>
 *   <li>{@code POST /api/admin/lifecycle/toggle?clusterId=...&nodeId=...&beanName=...&enabled=true|false}
 *       — toggle ENABLED для конкретной (node, bean) пары</li>
 *   <li>{@code POST /api/admin/lifecycle/leader?clusterId=...&nodeId=...&beanName=...} —
 *       назначить ноду лидером для bean'а (сбрасывает IS_LEADER у других нод)</li>
 * </ul>
 *
 * <p>Безопасность: эндпоинты должны быть закрыты cluster-internal network'ом
 * или AuthN/AuthZ слоем (не описано в этом контроллере — добавлять отдельно).
 */
@Slf4j
@RestController
@RequestMapping("/api/admin/lifecycle")
@RequiredArgsConstructor
public class LifecycleControlAdminController {

    private final IgniteClientFactory clientFactory;

    /**
     * Список всех бинов на всех нодах данного кластера.
     */
    @GetMapping
    public List<Map<String, Object>> list(@RequestParam String clusterId) {
        IgniteClient client = clientFactory.get(clusterId);
        List<Map<String, Object>> rows = new ArrayList<>();
        try (var cur = client.query(new org.apache.ignite.cache.query.SqlFieldsQuery(
                "SELECT NODE_ID, BEAN_NAME, ENABLED, IS_LEADER, UPDATED_AT" +
                " FROM LIFECYCLE_CONTROL.LIFECYCLE_CONTROL" +
                " ORDER BY NODE_ID, BEAN_NAME"))) {
            for (List<?> row : cur) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("nodeId", row.get(0));
                m.put("beanName", row.get(1));
                m.put("enabled", row.get(2));
                m.put("isLeader", row.get(3));
                m.put("updatedAt", row.get(4));
                rows.add(m);
            }
        }
        return rows;
    }

    /**
     * Toggle ENABLED для (nodeId, beanName). ContinuousQuery на стороне Ignite
     * подхватит UPDATE и вызовет {@code onEnabledChanged(enabled)} на бине.
     *
     * @return количество обновлённых строк (0 если такой пары нет, 1 если успех)
     */
    @PostMapping("/toggle")
    public Map<String, Object> toggle(
            @RequestParam String clusterId,
            @RequestParam String nodeId,
            @RequestParam String beanName,
            @RequestParam boolean enabled) {
        IgniteClient client = clientFactory.get(clusterId);
        Timestamp now = new Timestamp(System.currentTimeMillis());
        long updated;
        try (var cur = client.query(new org.apache.ignite.cache.query.SqlFieldsQuery(
                "UPDATE LIFECYCLE_CONTROL.LIFECYCLE_CONTROL" +
                "   SET ENABLED = ?, UPDATED_AT = ?" +
                " WHERE NODE_ID = ? AND BEAN_NAME = ?")
                .setArgs(enabled, now, nodeId, beanName))) {
            updated = ((Number) cur.iterator().next().get(0)).longValue();
        }
        log.info("[LifecycleAdmin] toggle cluster={} node={} bean={} enabled={} updated={}",
                clusterId, nodeId, beanName, enabled, updated);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("clusterId", clusterId);
        resp.put("nodeId", nodeId);
        resp.put("beanName", beanName);
        resp.put("enabled", enabled);
        resp.put("updated", updated);
        return resp;
    }

    /**
     * Передача leader-флага конкретной ноде для bean'а (cluster-singleton role).
     *
     * <p>Стратегия: сначала сбрасываем IS_LEADER=false у всех нод для этого
     * bean'а (одним UPDATE), затем выставляем IS_LEADER=true только указанной
     * (nodeId, beanName) паре. Eventually-consistent через CQ; в окне между
     * двумя UPDATE'ами теоретически могут быть 0 лидеров — это безопаснее
     * чем 2 лидера.
     *
     * <p>Использование: например ручной failover KafkaCdcPublisher с одной
     * ноды на другую.
     */
    @PostMapping("/leader")
    public Map<String, Object> setLeader(
            @RequestParam String clusterId,
            @RequestParam String nodeId,
            @RequestParam String beanName) {
        IgniteClient client = clientFactory.get(clusterId);
        Timestamp now = new Timestamp(System.currentTimeMillis());

        // 1. Снимаем leader-флаг со всех нод для этого bean'а
        long demoted;
        try (var cur = client.query(new org.apache.ignite.cache.query.SqlFieldsQuery(
                "UPDATE LIFECYCLE_CONTROL.LIFECYCLE_CONTROL" +
                "   SET IS_LEADER = false, UPDATED_AT = ?" +
                " WHERE BEAN_NAME = ? AND IS_LEADER = true")
                .setArgs(now, beanName))) {
            demoted = ((Number) cur.iterator().next().get(0)).longValue();
        }

        // 2. Назначаем нового лидера
        long promoted;
        try (var cur = client.query(new org.apache.ignite.cache.query.SqlFieldsQuery(
                "UPDATE LIFECYCLE_CONTROL.LIFECYCLE_CONTROL" +
                "   SET IS_LEADER = true, UPDATED_AT = ?" +
                " WHERE NODE_ID = ? AND BEAN_NAME = ?")
                .setArgs(now, nodeId, beanName))) {
            promoted = ((Number) cur.iterator().next().get(0)).longValue();
        }

        log.info("[LifecycleAdmin] setLeader cluster={} bean={} new-leader={} demoted={} promoted={}",
                clusterId, beanName, nodeId, demoted, promoted);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("clusterId", clusterId);
        resp.put("beanName", beanName);
        resp.put("newLeader", nodeId);
        resp.put("demoted", demoted);
        resp.put("promoted", promoted);
        return resp;
    }
}
