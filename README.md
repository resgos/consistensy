# stmnt-consistency-service

Dropapp-сервис сверки данных между Ignite-кластерами для ППРБ.Выписка.

## Архитектура

```
┌────────────────────────────────────────────┐
│   stmnt-consistency-service (Dropapp)      │
│                                            │
│  REST /api/consistency/*                   │
│         │                                  │
│         ▼                                  │
│  ConsistencyJob ── (3× thin-client) ──┐    │
│         │                              │   │
│         ▼                              ▼   │
│   PostgreSQL              Ignite-кластера  │
│   (runs, hashes,                 1 / 2 / 3 │
│    mismatches)                             │
└────────────────────────────────────────────┘
```

- Сервис **не пишет** в Ignite. Он только читает через **thin-client** все три кластера и считает MD5-хеш по бизнес-полям записей (правила нормализации — в `HashUtil`).
- Если хеши на одной бизнес-ключ-записи различаются между кластерами — это записывается в `consistency_mismatch`.
- Запуск: cron каждый час + REST `POST /api/consistency/run` для ad-hoc.

## Поддерживаемые кеши

| Кеш | Бизнес-ключ | Особенности |
|---|---|---|
| REGISTER | OBJECTID | — |
| TURN_DOC_CUR | OBJECTID | 2 режима (type50 / не-50); фильтр `CCOPERATIONDAY >= today − 3` |
| CLIENT | OBJECTID | — |
| CURRENCY | OBJECTID | — |
| DAY_BALANCES | REGISTER:CCOPERATIONDAY | tolerance 0.01 (округление до 2 знаков перед хешем); фильтр последних 3 дней |
| DIVISION | OBJECTID | — |
| INCOME_SALDO | OBJECTID | — |
| CB_RATE | OBJECTID | — |
| CASH_SYMBOL_DOC | OBJECTID | — |
| ~~ENRICH_DIRECTORY~~ | — | **пропущен**: требует `RQUID/CCVERSION` в DTO (см. доку) |

## REST API

### Consistency (сверка)

| | URL | Body / params | Назначение |
|---|---|---|---|
| `POST` | `/api/consistency/run` | `{"cacheName": "REGISTER"}` (опц.) | Запустить ad-hoc сверку; вернёт `{runId, status}` |
| `GET` | `/api/consistency/runs?limit=20&status=MISMATCH` | — | Список запусков |
| `GET` | `/api/consistency/runs/{id}` | — | Детали запуска |
| `GET` | `/api/consistency/mismatches?cacheName=&since=&unresolvedOnly=true&limit=100` | `since` — ISO instant | Список расхождений |
| `POST` | `/api/consistency/mismatches/{id}/resolve` | `{"notes": "fixed in ticket SBRF-12345"}` | Пометить разрешённым |
| `GET` | `/actuator/health` | — | Health |

### Admin fan-out на все кластера (init / cleanup / recalc)

Эти endpoint'ы вызывают `DayBalancesAdminService` (cluster-singleton Ignite-service) **параллельно на каждом из 3 кластеров** через thin-client `serviceProxy`. Используется для batch-операций при инициализации или обслуживании стенда.

| | URL | Body |
|---|---|---|
| `POST` | `/api/admin/init` | `{"registerId":"R001","fromDate":"2026-01-01","toDate":"2026-05-24","openingBalance":1000.00,"openingBalanceNat":null,"clusterIds":null}` |
| `POST` | `/api/admin/cleanup` | `{"beforeDate":"2025-11-25","registerFilter":null,"clusterIds":null}` |
| `POST` | `/api/admin/recalc` | `{"registerId":"R001","fromDate":"2026-05-20","toDate":"2026-05-24","clusterIds":null}` |

`clusterIds: null` → fan-out на все кластера из конфига. `clusterIds: ["cluster-1"]` → только на указанные.

**Init:**
- Максимум **180 дней** (`MAX_INIT_DAYS`).
- `openingBalance` (опц.) создаёт type50-якорь на `fromDate` с `EXPROP5='init'`. Якорь защищён от перезаписи штатным авто-пересчётом **и** от `cleanup`.
- Диапазон обрабатывается батчами по 30 дней (`INIT_BATCH_DAYS`), без раздува памяти кластера.

**Cleanup:**
- Удаляет `DAY_BALANCES` и `TURN_DOC_CUR` `CCTYPEOPER=50` записи с `CCOPERATIONDAY < beforeDate`.
- НЕ трогает type50 с `EXPROP5='init'` (init-якоря).
- `registerFilter: null` → все регистры; non-null → только указанный.

Пример ответа `/api/admin/init`:
```json
{
  "operation": "init",
  "perCluster": {
    "cluster-1": {"ok": true, "result": "InitResult{register=R001 from=2026-01-01 to=2026-05-24 days=144 batches=5 ok=true}"},
    "cluster-2": {"ok": true, "result": "InitResult{...}"},
    "cluster-3": {"ok": false, "error": "cluster not connected"}
  }
}
```

## Конфигурация (`application.yml`)

```yaml
consistency:
  cron: "0 5 * * * *"           # every hour at :05
  snapshot-lag: PT5M             # skip records modified < 5 min ago
  turn-lookback-days: 3          # TURN_DOC_CUR/DAY_BALANCES scope
  hash-retention-days: 30        # PG cleanup
  clusters:
    - id: cluster-1
      addresses: ["ignite-1:10800"]
    - id: cluster-2
      addresses: ["ignite-2:10800"]
    - id: cluster-3
      addresses: ["ignite-3:10800"]
```

Подключения управляются ENV: `CLUSTER{1,2,3}_HOST`, `CLUSTER{1,2,3}_PORT`, `PG_URL`, `PG_USER`, `PG_PASSWORD`.

## Локальный запуск

```bash
# 1. Собрать сервис
mvn -pl stmnt-consistency-service -am package -DskipTests

# 2. Поднять стек (3 Ignite + Postgres + service)
docker compose up -d

# 3. Залить тестовые данные во все 3 кластера
./scripts/seed-clusters.sh         # требует pyignite: pip install pyignite

# 4. Прогнать сверку — должно быть status=OK
curl -X POST http://localhost:8080/api/consistency/run

# 5. Внести расхождение в cluster-2 (currency RUB -> EUR на R001)
./scripts/inject-mismatch.sh

# 6. Прогнать снова — status=MISMATCH, mismatch_count=1
curl -X POST http://localhost:8080/api/consistency/run
curl 'http://localhost:8080/api/consistency/mismatches?unresolvedOnly=true'

# 7. Пометить разрешённым после починки
curl -X POST http://localhost:8080/api/consistency/mismatches/1/resolve \
     -H 'Content-Type: application/json' \
     -d '{"notes":"fixed via PR #123"}'
```

## PostgreSQL схема

- `consistency_run` — каждый прогон (запуск, статус, сводка).
- `consistency_hash` — детальные хеши `(cluster, cache, businessKey, hex)`. Для audit'а и для повторного сравнения исторических данных.
- `consistency_mismatch` — детектированные расхождения с разворачивающимся JSON `{cluster-1: hash, cluster-2: hash, cluster-3: hash}`.

Все три таблицы каскадно очищаются по retention (3:30 каждый день, `hash-retention-days` дней).

## Правила хеширования

См. `HashUtil`:
- `null` → `"NULL"`
- `BigDecimal` → `setScale(6, HALF_UP).toPlainString()` (для `DAY_BALANCES` — `setScale(2)`)
- `Date / Timestamp / LocalDate / LocalDateTime` → epoch millis (UTC, детерминированно)
- Поля соединяются через `|`, считается MD5, возвращается 32-символьная hex-строка

Порядок полей — строго по спецификации. **Любое изменение правил хеширования инвалидирует исторические записи** в `consistency_hash`.

## Mapping полей по кешам

См. `Hashers.java` — каждый класс соответствует одному кешу, поля и формат — точно по спецификации.

## Roadmap

- [ ] Алерты: при mismatch_count > 0 пушить в Slack/Telegram через webhook.
- [ ] Поле `CCVERSION/RQUID` в `ENRICH_DIRECTORY` DTO → включить в сверку.
- [ ] Streaming-режим: для больших объёмов читать и хешировать без накопления всей мапы в памяти.
- [ ] Snapshot-консистентность: использовать Ignite snapshot API вместо `snapshot-lag`.
