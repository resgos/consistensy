package ru.sbrf.pprb.stmnt.consistency.lib.events;

import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/**
 * Тонкая обёртка вокруг Spring's ApplicationEventPublisher.
 *
 * Доменный код вызывает {@link #publish(DomainEvents)} — событие летит как
 * Spring ApplicationEvent в текущей JVM. Дальше:
 *   - {@link KafkaEventListener} (если включён) подхватывает и шлёт в Kafka
 *   - любые @EventListener в проекте могут подписаться на внутрипроцессную
 *     реакцию (метрики, in-memory кеш и т.п.)
 *
 * Listener-pattern с in-process publish даёт нам:
 *  1. Domain logic не зависит от транспорта (Kafka / REST / гvocab)
 *  2. Можно полностью отключить Kafka (kafka-enabled=false) — события всё
 *     равно фиксятся через @EventListener'ы (например, для аудита)
 *  3. Кастомные подписчики добавляются без правки бизнес-логики
 */
@Component
@RequiredArgsConstructor
public class EventPublisher {

    private final ApplicationEventPublisher springPublisher;

    public void publish(DomainEvents event) {
        springPublisher.publishEvent(event);
    }
}
