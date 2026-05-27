# CDC-канал Ignite → Kafka → consistency-service: Smoke Report

## Архитектура

```
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│   ignite-1    │   │   ignite-2    │   │   ignite-3    │
│ (cluster-1)   │   │ (cluster-2)   │   │ (cluster-3)   │
│               │   │               │   │               │
│ KafkaCdcPubl. │   │ KafkaCdcPubl. │   │ KafkaCdcPubl. │
│  (CQ + Prod.) │   │  (CQ + Prod.) │   │  (CQ + Prod.) │
└──────┬────────┘   └──────┬────────┘   └──────┬────────┘
       │                   │                   │
       ▼                   ▼                   ▼
  topic:cdc.       topic:cdc.            topic:cdc.
  cluster-1.       cluster-2.            cluster-3.
  hashes           hashes                hashes
       │                   │                   │
       └───────────────────┼───────────────────┘
                           ▼
                ┌──────────────────────┐
                │  CdcConsumer         │  ← consistency-service (Spring Boot)
                │  @KafkaListener      │     topicPattern: cdc.cluster-.+\.hashes
                │  batch UPSERT        │     group: consistency-cdc
                └──────────┬───────────┘
                           ▼
                ┌──────────────────────┐
                │  consistency_hash_   │  ← Postgres
                │  latest              │     PK = (cluster_id, cache_name, business_key)
                └──────────┬───────────┘
                           ▼
                ┌──────────────────────┐
                │ ConsistencySweepJob  │  ← cron 0 */5 * * * *
                │ GROUP BY (cache,key) │
                │ HAVING COUNT(DIST    │
                │   hash) > 1          │
                │ OR  COUNT(DIST       │
                │   cluster_id) < N    │
                └──────────┬───────────┘
                           ▼
                   consistency_mismatch
                           │
                           ▼  EventPublisher → KafkaTemplate
                ┌──────────────────────┐
                │ stmnt-consistency.   │
                │   mismatch.detected  │  ← внешний топик (для downstream алертов)
                │   run.finished       │
                │   run.started        │
                │   error.recorded     │
                └──────────────────────┘
```

## Smoke-результаты (5 сценариев, прогон 2026-05-26)

Все сценарии выполнены через `/api/debug/cdc/scenario{N}-*` → прямой `cache.put()` через thin-client → ContinuousQuery в `KafkaCdcPublisherBean` → KafkaProducer → Kafka topic → `CdcConsumer` → `UPSERT consistency_hash_latest` → `ConsistencySweepJob` → `consistency_mismatch`.

### Состояние `consistency_hash_latest` после 5 сценариев

| cluster_id | cache_name | business_key | hash (short) | op       |
|------------|------------|--------------|--------------|----------|
| cluster-1  | REGISTER   | R001         | 4b8f6df3ae7e | PUT      |
| cluster-2  | REGISTER   | R001         | (empty)      | REMOVED  |
| cluster-3  | REGISTER   | R001         | 5602035ec332 | PUT      |
| cluster-1  | REGISTER   | R002         | fedcd11ec91e | PUT      |
| cluster-3  | REGISTER   | R002         | fedcd11ec91e | PUT      |
| cluster-1  | REGISTER   | R100         | 2c0295bab8d8 | PUT      |
| cluster-2  | REGISTER   | R100         | 2c0295bab8d8 | PUT      |
| cluster-3  | REGISTER   | R100         | 2c0295bab8d8 | PUT      |
| cluster-3  | REGISTER   | R999         | 96416f674f9b | PUT      |

### Результат `sweep` (`POST /api/consistency/run`)

```json
{"runId":2,"status":"SWEEP_MISMATCH"}
```

### Детектированные mismatch'и

| BK     | Сценарий          | Expected      | Detected | Hash-карта                                         |
|--------|-------------------|---------------|----------|----------------------------------------------------|
| R100   | identical         | NO MISMATCH   | **✓**    | (не попал в mismatches — все 3 hash равны)         |
| R001   | currency-diff + REMOVED | MISMATCH | **✓** | c1=4b8f, c2=__REMOVED__, c3=5602                  |
| R002   | missing on c2     | MISMATCH      | **✓**    | c1=fedcd, c3=fedcd (c2 отсутствует — one-sided)    |
| R999   | only on c3        | MISMATCH      | **✓**    | c3=9641 (c1/c2 отсутствуют — one-sided)            |

**4 из 4 сценариев** отработали как ожидалось.

## Что детектится sweep'ом (SQL предикат)

```sql
HAVING COUNT(DISTINCT CASE WHEN op='REMOVED' THEN '__REMOVED__' ELSE hash END) > 1
   OR  COUNT(DISTINCT cluster_id) < (SELECT COUNT(DISTINCT cluster_id) FROM consistency_hash_latest)
```

— первое условие ловит расхождения hash'ей включая REMOVED-tombstone;  
— второе ловит one-sided keys (присутствие в части кластеров, отсутствие в остальных).

## Kafka-топики после прогона

```
__consumer_offsets
stmnt-consistency.cdc.cluster-1.hashes      ← Ignite → consistency-service (CDC, inbound)
stmnt-consistency.cdc.cluster-2.hashes
stmnt-consistency.cdc.cluster-3.hashes
stmnt-consistency.mismatch.detected         ← consistency-service → внешние подписчики (outbound)
stmnt-consistency.run.finished
stmnt-consistency.run.started
```

### Пример inbound CDC-сообщения

```json
{"clusterId":"cluster-1","cacheName":"REGISTER","businessKey":"R001",
 "hash":"4b8f6df3ae7e4a47daf301a9ef7d615f","op":"PUT","ts":1779785597}
```

Kafka-key = `REGISTER:R001` (`partitionKey()`) — гарантирует упорядоченность событий по одному (cache, key).

### Пример outbound `mismatch.detected`

```json
{"runId":2,"cacheName":"REGISTER","businessKey":"R001",
 "clusterHashes":{"cluster-1":"4b8f...","cluster-2":"__REMOVED__","cluster-3":"5602..."},
 "detectedAt":1779785609.611236966}
```

### Пример outbound `run.finished`

```json
{"runId":2,"status":"SWEEP_MISMATCH","mismatchCount":10,"errorMessage":null,"finishedAt":1779785609.631908003}
```

## Ключевые ограничения и решения

1. **SQL DML через H2 engine не триггерит ContinuousQuery в Apache Ignite 2.16.** Поэтому старый `/api/debug/seed` (CREATE TABLE + DELETE/INSERT через SQL) НЕ генерит CDC-события. Для smoke-test'а CDC создан отдельный `CdcTestController` (`/api/debug/cdc/...`), использующий прямой `cache.put()` через thin-client `BinaryObjectBuilder`. В production-стенде бизнес-операции идут через cache API, CDC работает корректно.
2. **DROP TABLE инвалидирует cache + CQ.** Старый seed дропал каши перед re-create — это убивало все CQ. Заменено на idempotent `CREATE TABLE IF NOT EXISTS + DELETE`, cache живёт между вызовами.
3. **topicPattern subscription + новые топики.** Spring Kafka consumer с `topicPattern` подписывается только на топики, существующие на момент join. По дефолту `metadata.max.age.ms=300000` (5 мин). Снижено до 10 сек — consumer быстро увидит новые `cdc.cluster-N.hashes`.
4. **Java 8 target shared modules.** `apacheignite/ignite:2.16.0` docker-образ собран на Java 8 (bytecode v52). `stmnt-consistency-hashers` + `stmnt-consistency-ignite-cdc` собраны с `<release>8</release>`, `kafka-clients:3.4.1` (последняя версия с Java 8 support).
5. **JUL вместо SLF4J.** В uber-jar SLF4J binding'а нет, а Ignite использует JUL — `KafkaCdcPublisherBean` логирует через `java.util.logging.Logger`.
6. **Jackson FIELD visibility.** `CdcEvent` — record-like с field accessors (`clusterId()`, не `getClusterId()`). ObjectMapper настроен на `PropertyAccessor.FIELD, Visibility.ANY`.

## Состав CDC-инфраструктуры

| Артефакт                            | Назначение                                                                 |
|-------------------------------------|----------------------------------------------------------------------------|
| `stmnt-consistency-hashers/`        | Shared (Java 8): HashUtil, CdcEvent, CdcEventHasher                       |
| `stmnt-consistency-ignite-cdc/`     | Uber-jar для подкладки в libs/ Ignite-узла: KafkaCdcPublisherBean + 8 IgniteCdcHashers + kafka-clients + jackson |
| `docker/ignite-cdc.Dockerfile`      | apacheignite/ignite:2.16.0 + COPY uber-jar в libs/cdc-publisher.jar       |
| `docker/ignite-config-template.xml` | LifecycleBean для KafkaCdcPublisherBean; sys-property конфиг              |
| `docker-compose.yml`                | apache/kafka:3.7.0 KRaft + 3× stmnt-ignite-cdc + postgres + service       |
| `Flyway V4__cdc_hash_latest.sql`    | таблица `consistency_hash_latest` + индексы + `consistency_cdc_offset`    |
| `CdcConsumer`, `CdcKafkaConfig`     | @KafkaListener batch UPSERT, ErrorHandlingDeserializer<JsonDeserializer>  |
| `ConsistencySweepJob`               | cron `0 */5 * * * *`, GROUP BY (cache, key), пишет в consistency_mismatch + publish MismatchDetected event |
| `OffsetController`                  | POST `/api/consistency/offsets/seek?topic=...&partition=...&offset=...&reset=earliest|latest` через AdminClient.alterConsumerGroupOffsets |
| `CdcTestController`                 | POST `/api/debug/cdc/scenarioN-*` — прямые `cache.put` для smoke (минует SQL DML)  |

## Что осталось вне smoke

- Производственный шейп: в реальной системе бизнес-операции идут через cache API (или Service Proxy с cache.put внутри), а не через SQL DML — CDC работает «из коробки». Smoke-стенд требует CdcTestController как workaround.
- INCOME_SALDO — Postgres-таблица, не Ignite-cache. CDC-канал не покрывает; sweep увидит её только когда pull-mode включён (`CONSISTENCY_PULL_MODE=true`) и старый ConsistencyJob проходит SQL pull.
- Дедупликация в `consistency_mismatch`: каждый sweep сохраняет mismatch заново — в production нужен unique constraint `(cache_name, business_key, hashes_json)` или отдельная таблица "open mismatches" с lifecycle (open → resolved).
- `OffsetController.seek` для одного кластера: реализован через `AdminClient.alterConsumerGroupOffsets` (stop container → seek → start). Требует чтобы group не была активна — обеспечивается stop'ом + 2s waiting.

## Воспроизведение

```bash
cd stmnt-consistency
mvn -DskipTests install         # собирает 3 модуля + ставит в local repo
docker compose down -v
docker compose build
docker compose up -d

# wait for ignite + service ready
until curl -sf http://localhost:8080/actuator/health > /dev/null; do sleep 2; done
sleep 20  # CDC watch-loop навешивает CQ через ~5-15 сек

# seed (create Ignite caches)
curl -X POST http://localhost:8080/api/debug/seed -H "Content-Type: application/json" -d '{}'

# scenarios
curl -X POST http://localhost:8080/api/debug/cdc/scenario4-identical
curl -X POST http://localhost:8080/api/debug/cdc/scenario1-currency
curl -X POST http://localhost:8080/api/debug/cdc/scenario2-missing
curl -X POST http://localhost:8080/api/debug/cdc/scenario3-extra
curl -X POST http://localhost:8080/api/debug/cdc/scenario5-delete

# force sweep + show detected mismatches
curl -X POST http://localhost:8080/api/consistency/run -H "Content-Type: application/json" -d '{}'
docker exec consistency-pg psql -U consistency -d consistency \
  -c "SELECT cache_name, business_key, hashes_json::text FROM consistency_mismatch ORDER BY id DESC LIMIT 20;"

# tap inbound CDC stream
docker exec consistency-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic stmnt-consistency.cdc.cluster-1.hashes --from-beginning

# tap outbound events
docker exec consistency-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic stmnt-consistency.mismatch.detected --from-beginning
```
