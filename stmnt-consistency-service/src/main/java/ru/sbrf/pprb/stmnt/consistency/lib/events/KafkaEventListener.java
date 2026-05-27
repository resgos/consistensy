package ru.sbrf.pprb.stmnt.consistency.lib.events;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import ru.sbrf.pprb.stmnt.consistency.config.ConsistencyProperties;

/**
 * Подписан на {@link DomainEvents} через Spring @EventListener и публикует
 * в Kafka. Активируется только когда consistency.events.kafka-enabled=true.
 *
 * Если выключен — публикация наружу полностью отсутствует. In-process
 * Spring-события всё равно работают (другие подписчики могут реагировать).
 *
 * Topic naming:
 *   {prefix}.{topicSuffix}      где topicSuffix задаёт само событие
 *   Например: stmnt-consistency.run.finished, stmnt-consistency.mismatch.detected
 *
 * Key — event.eventKey() (например, runId), гарантирует упорядоченность в
 * рамках одной сущности (Kafka партиционирует по hash(key)).
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "consistency.events.kafka-enabled", havingValue = "true")
@RequiredArgsConstructor
public class KafkaEventListener {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ConsistencyProperties props;

    @Async
    @EventListener
    public void onDomainEvent(DomainEvents event) {
        String topic = props.getEvents().getTopicPrefix() + "." + event.topicSuffix();
        String key = event.eventKey();
        try {
            kafkaTemplate.send(topic, key, event)
                    .whenComplete((res, ex) -> {
                        if (ex != null) {
                            log.error("Failed to publish event topic={} key={}: {}",
                                    topic, key, ex.toString());
                        } else if (log.isDebugEnabled()) {
                            var meta = res.getRecordMetadata();
                            log.debug("Event published topic={} partition={} offset={} key={}",
                                    meta.topic(), meta.partition(), meta.offset(), key);
                        }
                    });
        } catch (Exception e) {
            // Не пробрасываем — публикация не должна валить бизнес-операцию.
            log.error("Kafka publish failed (sync) topic={} key={}: {}",
                    topic, key, e.toString(), e);
        }
    }
}
