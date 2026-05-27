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
import java.util.List;
import java.util.Map;

/**
 * Подписан на CDC-топики Ignite-кластеров.
 *
 * Топики устроены ОДИН-НА-КЛАСТЕР:
 *   stmnt-consistency.cdc.cluster-1.hashes
 *   stmnt-consistency.cdc.cluster-2.hashes
 *   stmnt-consistency.cdc.cluster-3.hashes
 *
 * Это позволяет двигать offset для одного кластера независимо от других
 * (через OffsetController.seek). consumer-group ОБЩАЯ — один процесс читает
 * все три топика.
 *
 * Поведение:
 *   - poll-batch UPSERT'ится в consistency_hash_latest.
 *   - clusterId в самом payload (CdcEvent.clusterId) — мы доверяем продьюсеру.
 *     Топик нужен только для управления offset'ами по кластеру.
 *
 * @ConditionalOnProperty: включается только когда consistency.cdc.enabled=true.
 * При false consistency-service работает только в pull-mode (старый ConsistencyJob).
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "consistency.cdc.enabled", havingValue = "true")
@RequiredArgsConstructor
public class CdcConsumer {

    private final HashLatestRepository hashLatest;
    private final ErrorRegistry errors;

    /**
     * Одна annotation, три топика — Spring создаст по одному
     * KafkaMessageListenerContainer на каждый. id'ы фиксируем, чтобы
     * OffsetController мог найти контейнер по cluster-id.
     */
    public static final String LISTENER_ID = "cdc-consumer";

    @KafkaListener(
            id = LISTENER_ID,
            idIsGroup = false,
            topicPattern = "${consistency.events.topic-prefix:stmnt-consistency}\\.cdc\\.cluster-.+\\.hashes",
            groupId = "${consistency.cdc.group-id:consistency-cdc}",
            containerFactory = "cdcKafkaListenerContainerFactory",
            batch = "true"
    )
    public void onBatch(List<ConsumerRecord<String, CdcEvent>> records) {
        if (records.isEmpty()) return;
        List<CdcEvent> events = new ArrayList<>(records.size());
        String firstTopic = records.get(0).topic();
        for (ConsumerRecord<String, CdcEvent> r : records) {
            CdcEvent e = r.value();
            if (e == null) {
                log.warn("CDC: null payload topic={} part={} offset={}",
                        r.topic(), r.partition(), r.offset());
                continue;
            }
            events.add(e);
        }
        try {
            hashLatest.upsertBatch(events);
            log.debug("CDC: upserted {} events topic={} first.offset={} last.offset={}",
                    events.size(), firstTopic,
                    records.get(0).offset(),
                    records.get(records.size() - 1).offset());
        } catch (Exception ex) {
            // Не глотаем — Spring Kafka не закоммитит offset, передёрнем batch.
            log.error("CDC: upsert batch failed topic={} size={}: {}",
                    firstTopic, events.size(), ex.toString(), ex);
            errors.error("cdc_consumer", "UPSERT_FAILED",
                    "CDC batch upsert failed", ex,
                    Map.of("topic", firstTopic, "batchSize", events.size()));
            throw ex;
        }
    }

}
