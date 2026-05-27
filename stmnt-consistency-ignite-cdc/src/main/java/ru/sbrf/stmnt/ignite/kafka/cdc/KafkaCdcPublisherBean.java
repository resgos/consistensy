package ru.sbrf.stmnt.ignite.kafka.cdc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.sbrf.pprb.stmnt.consistency.hashers.CdcEvent;
import ru.sbrf.pprb.stmnt.consistency.hashers.CdcEventHasher;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * CDC-producer на стороне Ignite-узла.
 *
 * <h3>Почему IgniteEvents, а не ContinuousQuery</h3>
 * В Apache Ignite 2.16 ContinuousQuery имеет регрессию: SQL DML (UPDATE/INSERT/
 * DELETE через H2 engine) не всегда срабатывает local listener — известная
 * проблема, см. IGNITE-9586. Direct cache API (cache.put/remove/invoke) — работает.
 *
 * IgniteEvents с {@code EVT_CACHE_OBJECT_PUT/REMOVED} устроены ниже — это
 * notifications прямо из {@code GridCacheMapEntry.innerUpdate}, ловят ВСЁ:
 *   - cache.put / putAll / remove / invoke / replace
 *   - SQL INSERT / UPDATE / DELETE
 *   - DataStreamer batched updates (если includeOverwrite=true)
 *   - CacheStore write-through callbacks
 *
 * Цена: ignite-config-template.xml должен включать эти события через
 * {@code <property name="includeEventTypes">}.
 *
 * <h3>Поведение</h3>
 * - {@code AFTER_NODE_START} → KafkaProducer + одна подписка
 *   {@code ignite.events().localListen(...)} на все кеши сразу
 * - В callback: матчим cacheName c регистром hashers; вычисляем hash;
 *   шлём в Kafka topic {@code {prefix}.cdc.{clusterId}.hashes}
 * - {@code BEFORE_NODE_STOP} → stopLocalListen + producer.close()
 *
 * REMOVED → пустой hash + op=REMOVED (consumer трактует как tombstone).
 *
 * Конфигурация через setter'ы из ignite-local.xml: bootstrapServers, clusterId,
 * topicPrefix, hashers, enabled.
 */
public class KafkaCdcPublisherBean implements LifecycleBean {

    private static final Logger log = Logger.getLogger(KafkaCdcPublisherBean.class.getName());

    @IgniteInstanceResource
    private Ignite ignite;

    // ---- configuration -----------------------------------------------------
    private String bootstrapServers;
    private String clusterId;
    private String topicPrefix = "stmnt-consistency";
    private Map<String, CdcEventHasher> hashers = new HashMap<>();
    private boolean enabled = true;

    // ---- runtime -----------------------------------------------------------
    private Producer<String, String> producer;
    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    private IgnitePredicate<Event> listener;

    // Cache name aliases: SQL_PUBLIC_REGISTER / SQL_REGISTER_REGISTER / REGISTER → logical "REGISTER".
    // Заполняется лениво при первом событии для нового cache name.
    private final Map<String, String> physicalToLogical = new HashMap<>();

    // ---- LifecycleBean ----------------------------------------------------

    @Override
    public void onLifecycleEvent(LifecycleEventType evt) {
        log.info("CDC publisher: onLifecycleEvent " + evt + " enabled=" + enabled);
        if (!enabled) return;
        if (evt == LifecycleEventType.AFTER_NODE_START) {
            startProducer();
            startEventListener();
        } else if (evt == LifecycleEventType.BEFORE_NODE_STOP) {
            stop();
        }
    }

    private void startProducer() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "cdc-publisher-" + clusterId);
        producer = new KafkaProducer<>(p);
        log.info("CDC publisher: producer started clusterId=" + clusterId
                + " bootstrap=" + bootstrapServers + " hashers=" + hashers.keySet());
    }

    private void startEventListener() {
        IgniteEvents events = ignite.events();
        // localListen: predicate выполняется на ЭТОМ узле — не нужна сериализация
        // (remoteListen маршалит predicate во весь кластер, наш bean не serializable).
        // Для single-node-per-cluster архитектуры этого достаточно: каждый узел
        // публикует события своего cache в свой Kafka topic.
        listener = ev -> {
            if (ev instanceof CacheEvent) {
                onCacheEvent((CacheEvent) ev);
            }
            return true;
        };
        events.localListen(listener,
                EventType.EVT_CACHE_OBJECT_PUT,
                EventType.EVT_CACHE_OBJECT_REMOVED);
        log.info("CDC publisher: local event listener registered for EVT_CACHE_OBJECT_PUT/REMOVED");
    }

    /**
     * Поскольку remoteListen вешает callback'а на всех узлах кластера — нам
     * нужно отфильтровать только наши целевые caches (по logical name).
     *
     * @return true чтобы листенер продолжил слушать (false означал бы deregister)
     */
    private void onCacheEvent(CacheEvent ev) {
        try {
            String physicalCache = ev.cacheName();
            log.info("CDC onCacheEvent cache=" + physicalCache
                    + " type=" + ev.type() + " key=" + ev.key());
            String logical = resolveLogical(physicalCache);
            if (logical == null) {
                log.info("CDC skipped (logical=null) cache=" + physicalCache);
                return;
            }
            CdcEventHasher hasher = hashers.get(logical);
            if (hasher == null) {
                log.info("CDC skipped (no hasher) logical=" + logical);
                return;
            }

            CdcEvent.Op op = ev.type() == EventType.EVT_CACHE_OBJECT_REMOVED
                    ? CdcEvent.Op.REMOVED
                    : CdcEvent.Op.PUT;
            Object key = ev.key();
            Object value = (op == CdcEvent.Op.REMOVED) ? ev.oldValue() : ev.newValue();
            // Для DML через SQL value/oldValue могут быть BinaryObject или DTO — наши
            // hasher'ы умеют оба пути (через BinaryObject.field или reflection getter).
            String bk = hasher.businessKeyOf(key, value);
            if (bk == null) {
                if (log.isLoggable(Level.FINE)) {
                    log.fine("CDC: null bk cache=" + logical + " op=" + op);
                }
                return;
            }
            String hash = (op == CdcEvent.Op.REMOVED || value == null)
                    ? ""
                    : hasher.hashOf(key, value);
            String topic = topicPrefix + ".cdc." + clusterId + ".hashes";
            CdcEvent payload = new CdcEvent(clusterId, logical, bk, hash, op,
                    System.currentTimeMillis());
            String json = mapper.writeValueAsString(payload);
            if (log.isLoggable(Level.FINE)) {
                log.fine("CDC send topic=" + topic + " key=" + payload.partitionKey()
                        + " op=" + op);
            }
            producer.send(new ProducerRecord<>(topic, payload.partitionKey(), json),
                    (md, ex) -> {
                        if (ex != null) {
                            log.log(Level.SEVERE,
                                    "CDC publish failed topic=" + topic + " key=" + payload.partitionKey(),
                                    ex);
                        }
                    });
        } catch (Exception e) {
            log.log(Level.SEVERE, "CDC dispatch failed for event " + ev, e);
        }
    }

    /** Мапим физическое имя кеша в логическое (REGISTER, TURN_DOC_CUR, …). */
    private String resolveLogical(String physical) {
        if (physical == null) return null;
        String cached = physicalToLogical.get(physical);
        if (cached != null) return cached.isEmpty() ? null : cached;
        // Проверяем точное совпадение
        if (hashers.containsKey(physical)) {
            physicalToLogical.put(physical, physical);
            return physical;
        }
        // SQL_<schema>_<table> или SQL_<schema> формы
        for (String logical : hashers.keySet()) {
            if (physical.equals("SQL_" + logical + "_" + logical)
                    || physical.equals("SQL_PUBLIC_" + logical)
                    || physical.equals("SQL_" + logical)) {
                physicalToLogical.put(physical, logical);
                return logical;
            }
        }
        // Запомним что этот cache нам неинтересен — больше не ищем.
        physicalToLogical.put(physical, "");
        return null;
    }

    private void stop() {
        if (listener != null) {
            try { ignite.events().stopLocalListen(listener); } catch (Exception ignore) {}
            listener = null;
        }
        if (producer != null) {
            try { producer.flush(); producer.close(); } catch (Exception ignore) {}
            producer = null;
        }
        log.info("CDC publisher: stopped clusterId=" + clusterId);
    }

    // ---- setters -----------------------------------------------------------
    public void setBootstrapServers(String v) { this.bootstrapServers = v; }
    public void setClusterId(String v)        { this.clusterId = v; }
    public void setTopicPrefix(String v)      { this.topicPrefix = v; }
    public void setHashers(Map<String, CdcEventHasher> v) { this.hashers = v; }
    public void setEnabled(boolean v)         { this.enabled = v; }
}
