package ru.sbrf.pprb.stmnt.consistency.rpc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.*;
import ru.sbrf.pprb.stmnt.consistency.lib.cdc.CdcConsumer;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Управление CDC consumer offset'ами.
 *
 *   POST /api/consistency/offsets/seek
 *        ?topic=stmnt-consistency.cdc.cluster-2.hashes
 *        &partition=0
 *        &offset=12345                  (literal offset)
 *        — or —
 *        &reset=earliest|latest         (well-known positions)
 *
 * Алгоритм:
 *   1. stop() container — KafkaConsumer выходит из group, оставляет partition
 *   2. AdminClient.alterConsumerGroupOffsets(group, {tp -> offset})
 *      — этот вызов работает только если group не активна (consumer'ов нет)
 *   3. start() container — присоединяется заново, читает с проставленного offset
 *
 *   GET /api/consistency/offsets/status — состояние listener + position по partition.
 */
@Slf4j
@RestController
@RequestMapping("/api/consistency/offsets")
@ConditionalOnProperty(name = "consistency.cdc.enabled", havingValue = "true")
@RequiredArgsConstructor
public class OffsetController {

    private final KafkaListenerEndpointRegistry registry;

    @Value("${spring.kafka.bootstrap-servers:kafka:9092}")
    private String bootstrap;

    @Value("${consistency.cdc.group-id:consistency-cdc}")
    private String groupId;

    @PostMapping("/seek")
    public Map<String, Object> seek(@RequestParam String topic,
                                     @RequestParam int partition,
                                     @RequestParam(required = false) Long offset,
                                     @RequestParam(required = false) String reset) {
        if (offset == null && reset == null) {
            return Map.of("ok", false, "error", "specify either 'offset' or 'reset=earliest|latest'");
        }
        MessageListenerContainer container = registry.getListenerContainer(CdcConsumer.LISTENER_ID);
        if (container == null) {
            return Map.of("ok", false, "error", "cdc listener not registered");
        }

        TopicPartition tp = new TopicPartition(topic, partition);
        try (AdminClient admin = AdminClient.create(adminProps())) {
            // 1) Останавливаем consumer — group становится неактивной.
            log.warn("Offset seek: stopping container topic={} partition={}", topic, partition);
            container.stop();
            // Спиннер — alterConsumerGroupOffsets откажет если group ещё активна.
            waitGroupInactive(2_000);

            // 2) Резолвим target-offset.
            long targetOffset;
            if (offset != null) {
                targetOffset = offset;
            } else if ("earliest".equalsIgnoreCase(reset)) {
                targetOffset = resolveOffset(admin, tp, OffsetSpec.earliest());
            } else if ("latest".equalsIgnoreCase(reset)) {
                targetOffset = resolveOffset(admin, tp, OffsetSpec.latest());
            } else {
                return Map.of("ok", false, "error", "unknown reset='" + reset + "'");
            }

            // 3) Прописываем offset в Kafka.
            admin.alterConsumerGroupOffsets(groupId,
                    Collections.singletonMap(tp, new OffsetAndMetadata(targetOffset)))
                    .all().get(10, TimeUnit.SECONDS);
            log.warn("Offset seek: alterConsumerGroupOffsets ok group={} {}={}",
                    groupId, tp, targetOffset);

            return Map.of("ok", true,
                    "topic", topic, "partition", partition,
                    "newOffset", targetOffset,
                    "group", groupId);
        } catch (Exception e) {
            log.error("Offset seek failed topic={} part={}: {}", topic, partition, e.toString(), e);
            return Map.of("ok", false, "error", e.toString());
        } finally {
            // 4) Стартуем контейнер обратно.
            try { container.start(); } catch (Exception ignore) {}
        }
    }

    private long resolveOffset(AdminClient admin, TopicPartition tp, OffsetSpec spec) throws Exception {
        ListOffsetsResult.ListOffsetsResultInfo info = admin
                .listOffsets(Collections.singletonMap(tp, spec))
                .partitionResult(tp).get(5, TimeUnit.SECONDS);
        return info.offset();
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        MessageListenerContainer container = registry.getListenerContainer(CdcConsumer.LISTENER_ID);
        if (container == null) return Map.of("running", false);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("running", container.isRunning());
        out.put("groupId", container.getGroupId());
        out.put("listenerId", CdcConsumer.LISTENER_ID);
        // Committed-offset'ы через AdminClient — best-effort.
        try (AdminClient admin = AdminClient.create(adminProps())) {
            var committed = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
            Map<String, Long> offsets = new LinkedHashMap<>();
            committed.forEach((tp, om) -> offsets.put(
                    tp.topic() + ":" + tp.partition(), om.offset()));
            out.put("committedOffsets", offsets);
        } catch (Exception e) {
            out.put("committedOffsets.error", e.toString());
        }
        return out;
    }

    private Properties adminProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("client.id", "consistency-offset-admin");
        return p;
    }

    /** Простой спиннер — даём Kafka время заметить, что consumer ушёл. */
    private void waitGroupInactive(long maxMillis) {
        try { Thread.sleep(maxMillis); } catch (InterruptedException ignore) {}
    }
}
