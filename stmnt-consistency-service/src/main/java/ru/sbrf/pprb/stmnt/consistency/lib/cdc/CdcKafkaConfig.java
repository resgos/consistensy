package ru.sbrf.pprb.stmnt.consistency.lib.cdc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import ru.sbrf.pprb.stmnt.consistency.hashers.CdcEvent;

import java.util.HashMap;
import java.util.Map;

/**
 * Конфиг Kafka-consumer'а для CDC-потока.
 *
 * Активируется только когда consistency.cdc.enabled=true. Иначе бины не
 * создаются и CdcConsumer не стартует.
 *
 * JsonDeserializer обёрнут в ErrorHandlingDeserializer — кривое сообщение
 * не валит весь listener, только конкретную запись (она попадёт в DLT
 * если настроишь, либо просто залогируется как ошибка десериализации).
 */
@Configuration
@EnableKafka
@ConditionalOnProperty(name = "consistency.cdc.enabled", havingValue = "true")
public class CdcKafkaConfig {

    @Value("${spring.kafka.bootstrap-servers:kafka:9092}")
    private String bootstrap;

    @Value("${consistency.cdc.group-id:consistency-cdc}")
    private String groupId;

    @Value("${consistency.cdc.max-poll-records:500}")
    private int maxPollRecords;

    @Bean
    public ConsumerFactory<String, CdcEvent> cdcConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        // topicPattern: consumer должен периодически refresh метаданные чтобы
        // увидеть новые топики (cdc.cluster-N.hashes) когда producer'ы стартуют.
        // По дефолту 5 минут — слишком долго для smoke.
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 10_000);
        // ErrorHandlingDeserializer пропускает кривой payload (не валит pipeline).
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        // Доверяем нашему пакету hashers — CdcEvent оттуда.
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "ru.sbrf.pprb.stmnt.consistency.hashers");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, CdcEvent.class.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /** Фабрика batch-listener'ов (см. CdcConsumer.onBatch). */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, CdcEvent> cdcKafkaListenerContainerFactory(
            ConsumerFactory<String, CdcEvent> cdcConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, CdcEvent> f =
                new ConcurrentKafkaListenerContainerFactory<>();
        f.setConsumerFactory(cdcConsumerFactory);
        f.setBatchListener(true);
        f.setConcurrency(1);  // один поток на topic-pattern; масштабируется через partition'ы
        return f;
    }
}
