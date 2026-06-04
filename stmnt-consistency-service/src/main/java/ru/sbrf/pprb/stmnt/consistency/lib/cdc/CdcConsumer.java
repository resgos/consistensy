package ru.sbrf.pprb.stmnt.consistency.lib.cdc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.sbrf.pprb.stmnt.consistency.hashers.CdcEvent;
import ru.sbrf.pprb.stmnt.consistency.lib.ErrorRegistry;
import ru.sbrf.pprb.stmnt.consistency.lib.repo.HashLatestRepository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CDC consumer — подписан на ОДИН Kafka topic для всех CDC events
 * со всех Ignite кешей и кластеров.
 *
 * <h3>Архитектура</h3>
 * Producer: {@code KafkaCdcPublisherBean} (на каждой Ignite-ноде) публикует
 * в {@code stmnt-consistency-cdc} (имя настраивается через
 * {@code consistency.cdc.topic}).
 * Payload: {@link CdcEvent} с полем {@code clusterId} — consumer определяет
 * источник по нему (НЕ по topic name, как было раньше с per-cluster схемой).
 *
 * <h3>Раньше</h3>
 * Было N топиков {@code stmnt-consistency.cdc.cluster-N.hashes} (по одному
 * на кластер), consumer подписывался через {@code topicPattern}. Это требовало
 * создавать новый topic при добавлении кластера + DBA overhead.
 *
 * <h3>Сейчас</h3>
 * Один topic для всех. {@code clusterId} в payload — consumer группирует
 * по нему когда нужно (например для retention policies или alerting per-cluster).
 *
 * <h3>Поведение</h3>
 * <ul>
 *   <li>poll-batch UPSERT'ится в {@code consistency_hash_latest}.</li>
 *   <li>{@code (cacheName, businessKey)} → последний hash для каждой записи
 *       — sweep job находит расхождения через GROUP BY на этой таблице.</li>
 *   <li>Партиционирование в Kafka по {@code clusterId:businessKey} —
 *       упорядоченность в рамках одной (cluster, register) пары сохраняется.</li>
 * </ul>
 *
 * <p>{@code @ConditionalOnProperty}: включается только когда
 * {@code consistency.cdc.enabled=true}. При false consistency-service работает
 * только в pull-mode (старый {@code ConsistencyJob}).
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "consistency.cdc.enabled", havingValue = "true")
@RequiredArgsConstructor
public class CdcConsumer {

    private final HashLatestRepository hashLatest;
    private final ErrorRegistry errors;

    /** Listener ID — для {@code OffsetController} (найти container по имени). */
    public static final String LISTENER_ID = "cdc-consumer";

    @KafkaListener(
            id = LISTENER_ID,
            idIsGroup = false,
            // Один topic. Property consistency.cdc.topic — синхронизирован с
            // KafkaCdcPublisherBean.topic на стороне Ignite.
            topics = "${consistency.cdc.topic:stmnt-consistency-cdc}",
            groupId = "${consistency.cdc.group-id:consistency-cdc}",
            containerFactory = "cdcKafkaListenerContainerFactory",
            batch = "true"
    )
    public void onBatch(List<ConsumerRecord<String, CdcEvent>> records) {
        if (records.isEmpty()) return;
        List<CdcEvent> events = new ArrayList<>(records.size());
        String firstTopic = records.get(0).topic();
        // Кол-во events по clusterId — для observability и log'ов.
        Map<String, Integer> perCluster = new HashMap<>();
        for (ConsumerRecord<String, CdcEvent> r : records) {
            CdcEvent e = r.value();
            if (e == null) {
                log.warn("CDC: null payload topic={} part={} offset={}",
                        r.topic(), r.partition(), r.offset());
                continue;
            }
            events.add(e);
            // clusterId из payload — НЕ из topic name (топик теперь один).
            perCluster.merge(
                    e.clusterId() != null ? e.clusterId() : "UNKNOWN",
                    1, Integer::sum);
        }
        try {
            hashLatest.upsertBatch(events);
            if (log.isDebugEnabled()) {
                log.debug("CDC: upserted {} events topic={} byCluster={} first.offset={} last.offset={}",
                        events.size(), firstTopic, perCluster,
                        records.get(0).offset(),
                        records.get(records.size() - 1).offset());
            }
        } catch (Exception ex) {
            // Не глотаем — Spring Kafka не закоммитит offset, передёрнет batch.
            log.error("CDC: upsert batch failed topic={} size={} byCluster={}: {}",
                    firstTopic, events.size(), perCluster, ex.toString(), ex);
            errors.error("cdc_consumer", "UPSERT_FAILED",
                    "CDC batch upsert failed", ex,
                    Map.of("topic", firstTopic, "batchSize", events.size(),
                            "byCluster", perCluster));
            throw ex;
        }
    }

}
