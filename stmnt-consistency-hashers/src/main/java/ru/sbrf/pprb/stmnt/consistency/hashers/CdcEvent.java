package ru.sbrf.pprb.stmnt.consistency.hashers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * SHARED. Полезная нагрузка одного CDC-сообщения в Kafka.
 *
 * Контракт топика {prefix}.cdc.{clusterId}.hashes:
 *   key   (Kafka)  : cacheName + ":" + businessKey         // для упорядоченности
 *   value (Kafka)  : CdcEvent JSON
 *
 * Поля:
 *   clusterId    — какой Ignite-кластер сгенерировал событие (cluster-1 / -2 / -3)
 *   cacheName    — логическое имя кеша (REGISTER, TURN_DOC_CUR, …)
 *   businessKey  — стабильный бизнес-ключ для cross-cluster сравнения
 *                  (для DAY_BALANCES это "register:operationDay", для большинства —
 *                  просто objectId)
 *   hash         — md5 канонической формы значения, см. HashUtil
 *   op           — операция кеша: PUT / REMOVED
 *   ts           — момент события (epoch millis UTC)
 *
 * Plain Java record-like (без Lombok) чтобы Ignite-lib (Java 11, нет records) мог
 * использовать. На Java 17 стороне Jackson десериализует через @JsonCreator.
 */
public final class CdcEvent {

    public enum Op { PUT, REMOVED }

    private final String clusterId;
    private final String cacheName;
    private final String businessKey;
    private final String hash;
    private final Op op;
    private final long ts;

    @JsonCreator
    public CdcEvent(@JsonProperty("clusterId")   String clusterId,
                    @JsonProperty("cacheName")   String cacheName,
                    @JsonProperty("businessKey") String businessKey,
                    @JsonProperty("hash")        String hash,
                    @JsonProperty("op")          Op op,
                    @JsonProperty("ts")          long ts) {
        this.clusterId = clusterId;
        this.cacheName = cacheName;
        this.businessKey = businessKey;
        this.hash = hash;
        this.op = op;
        this.ts = ts;
    }

    public String clusterId()    { return clusterId; }
    public String cacheName()    { return cacheName; }
    public String businessKey()  { return businessKey; }
    public String hash()         { return hash; }
    public Op op()               { return op; }
    public long ts()             { return ts; }

    /** Kafka partitioning key. Гарантирует упорядоченность событий по одному (cache, key). */
    public String partitionKey() {
        return cacheName + ":" + businessKey;
    }

    @Override
    public String toString() {
        return "CdcEvent{" + clusterId + "/" + cacheName + "/" + businessKey
                + " " + op + " " + hash + " @" + ts + "}";
    }
}
