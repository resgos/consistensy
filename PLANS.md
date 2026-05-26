# Snapshot планов всех SQL-запросов (Apache Ignite OSS 2.16, single-node)

> **Версия**: Apache Ignite **2.16.0** (OSS). Платформа V Datagrid 17.6.3 ещё не настроена — ждём Maven-coordinate.

Стенд: single-node `apacheignite/ignite:2.16.0` × 3 кластера. Tables создаются через SQL-DDL (`CREATE TABLE ... WITH CACHE_NAME=...`). Индексы — только PRIMARY KEY + два явных композитных на `TURNDOCCUR`:
- `IDX_TDC_REG_OP (REGISTER, CCOPERATIONDAY)`
- `IDX_TDC_REG_TYPE_OP (REGISTER, CCTYPEOPER, CCOPERATIONDAY)`

Получено через `GET /api/debug/explain-all?clusterId=cluster-1`.

---

## Сводка

| Категория | Запросов | __SCAN_ | Индекс | Ошибка |
|---|---|---|---|---|
| hasher.* (9) | 9 | **9** | 0 | 0 |
| daybalances.* SELECT (14) | 14 | 5 | **7** | 2 (Calcite не подключён в OSS) |

Ничего пока **не правится** — только инвентаризация.

---

## Hasher-запросы (consistency-service)

Все 9 — `SELECT ... FROM <TABLE>` (полная выгрузка для построения хеша) или `WHERE CCOPERATIONDAY >= ?`. Везде **`__SCAN_`** — потому что в smoke seed нет одиночных индексов по CCOPERATIONDAY. В проде через `QueryEntity.indexes` план изменится.

### 1. `hasher.REGISTER`
```
Map:    PUBLIC.REGISTER.__SCAN_
Reduce: PUBLIC."merge_scan"
```

### 2. `hasher.TURN_DOC_CUR` — **WHERE CCOPERATIONDAY >= ?**
```
Map:    PUBLIC.TURNDOCCUR.__SCAN_ + filter (CCOPERATIONDAY >= ?)
Reduce: PUBLIC."merge_scan"
```
⚠️ Лидирующий столбец `IDX_TDC_REG_OP` — `REGISTER`, поэтому фильтр только по `CCOPERATIONDAY` не использует индекс. В проде нужен **`IDX_TDC_OPDAY (CCOPERATIONDAY)`** или одиночный `@QuerySqlField(index=true)` на `ccOperationDay`.

### 3. `hasher.CLIENT`, `hasher.CURRENCY`, `hasher.DIVISION`, `hasher.INCOME_SALDO`, `hasher.CB_RATE`, `hasher.CASH_SYMBOL_DOC`
Все — `__SCAN_` + `merge_scan`. Это **корректно** для маленьких таблиц-справочников (полная выгрузка для хеша). Индексы нужны были бы только если бы фильтровали.

### 4. `hasher.DAY_BALANCES` — **WHERE CCOPERATIONDAY >= ?**
```
Map:    PUBLIC.DAYBALANCES.__SCAN_ + filter
Reduce: PUBLIC."merge_scan"
```
⚠️ Та же проблема: PK = `(REGISTER, CCOPERATIONDAY)`, лидирующий — `REGISTER`. Фильтр только по дате не использует PK. В проде хотелось бы **`IDX_DB_OPDAY (CCOPERATIONDAY)`**.

---

## DayBalancesRecalcService SQL (Ignite-side)

### `SQL_REGISTERS_FOR_RECALC`
**ERROR**: `Column "R.CCDAYBALANCESBEGINDATE" not found`
В smoke seed колонка не объявлена. В проде (`Register.java` DTO) она есть. План снимется только на реальном кластере.

### `SQL_ACTIVE_REGISTERS`
```
Map:    PUBLIC.REGISTER.__SCAN_ + filter (CCOPENDATE / CCCLOSEDATE)
Reduce: PUBLIC."merge_scan"
```
⚠️ Full scan по REGISTER. В проде регистров может быть до миллионов. Нужен **индекс на `(CCOPENDATE, CCCLOSEDATE)`** или хотя бы на `CCBALANCERECALCDATE` (фильтр в `SQL_REGISTERS_FOR_RECALC`).

### `SQL_DAY_AGGREGATES_RANGE`
```
Map:
  UNION ALL (CCTYPEOPER=0, CCTYPEOPER=40):
    PUBLIC.IDX_TDC_REG_TYPE_OP: REGISTER='R001' AND CCTYPEOPER=0 AND CCOPERATIONDAY BETWEEN ...
    PUBLIC.IDX_TDC_REG_TYPE_OP: REGISTER='R001' AND CCTYPEOPER=40 AND CCOPERATIONDAY BETWEEN ...
  GROUP BY CCOPERATIONDAY, CCDT
Reduce: GROUP BY + SUM/COUNT/MAX
```
✅ **Идеально**: оба ветвления UNION ALL пробивают композитный индекс `(REGISTER, CCTYPEOPER, CCOPERATIONDAY)` с тремя equality- и range-предикатами.

### `SQL_START_SUM_SV4`, `SQL_SUM_BETWEEN`
```
PUBLIC.IDX_TDC_REG_TYPE_OP: REGISTER='R001' AND CCTYPEOPER IN(0,40) AND CCOPERATIONDAY < / BETWEEN ...
```
✅ Индекс используется. `IN(0,40)` разбивается на range-сканы.

### `SQL_PREV_OPER_DATE`
**ERROR**: `Query engines not configured, but specified engine: calcite`
Apache Ignite OSS 2.16 не содержит `ignite-calcite` в дефолтном image. В **stmnt-ignite_precalc** через `ignite-local.xml` подключен `<CalciteQueryEngineConfiguration>` — там план получим. В smoke же без Calcite запрос упадёт. Hint `/*+ QUERY_ENGINE('calcite') */` нельзя проверить без Calcite engine.

### `SQL_FIND_TYPE50`, `SQL_TYPE50_START_ON_DATE`
```
PUBLIC.IDX_TDC_REG_TYPE_OP: REGISTER='R001' AND CCTYPEOPER=50 AND CCOPERATIONDAY = ?
LIMIT 1
```
✅ Точечный lookup через композитный индекс. `LIMIT 1` отрабатывает на map-фазе.

### `SQL_TYPE50_START_BEFORE`
**ERROR**: `Query engines not configured, but specified engine: calcite` — то же, что `SQL_PREV_OPER_DATE`. В prod с Calcite этот запрос должен показать `INDEX REVERSE_SCAN`.

### `SQL_CB_RATE`
```
PUBLIC.CBRATE.__SCAN_ + filter (CCCODE=? AND CCDATE=?)
```
⚠️ Full scan по CBRATE. В проде нужен **composite (CCCODE, CCDATE)** для O(log n) lookup.

### `SQL_COUNT_NONTYPE50_ON_DAY`
```
PUBLIC.IDX_TDC_REG_TYPE_OP: REGISTER='R001' AND CCTYPEOPER IN(0,40) AND CCOPERATIONDAY = ?
COUNT(*)
```
✅ Индекс используется.

### `findRegistersWithActivityOnDay` — `SELECT DISTINCT REGISTER ... WHERE CCOPERATIONDAY=?`
```
PUBLIC.TURNDOCCUR.__SCAN_ + filter (CCOPERATIONDAY=?)
SELECT DISTINCT
```
⚠️ Full scan. Нет одиночного индекса по `CCOPERATIONDAY` (только композитные с лидирующим `REGISTER`). В проде вызывается в `dailyCleanupOnce` 1 раз/день, поэтому критичность низкая.

### `findAllPrimaryRegistersWithType50` — `SELECT DISTINCT REGISTER WHERE CCTYPEOPER=50`
```
PUBLIC.TURNDOCCUR.__SCAN_ + filter (CCTYPEOPER=50)
SELECT DISTINCT
```
⚠️ Full scan по миллиардам строк, чтобы найти ~100 type50-записей. Нужен **индекс на `CCTYPEOPER`** или композит с `CCTYPEOPER` лидирующим. После моего фикса детерминированных ccIdEKS у каждого регистра ровно 1 type50 → запрос всё равно «дешёвый» по результату, но дорогой по сканированию. Кандидат на оптимизацию.

### `pruneOldType50_max` — `SELECT MAX(CCOPERATIONDAY) WHERE REGISTER=? AND CCTYPEOPER=50`
```
PUBLIC.IDX_TDC_REG_TYPE_OP: REGISTER='R001' AND CCTYPEOPER=50
MAX(CCOPERATIONDAY)
```
✅ Индекс используется. MAX через range scan c ранним выходом.

---

## Рекомендуемые индексы (НЕ применять пока)

Список потенциальных индексов для production (зафиксировать после согласования):

| Индекс | Покрывает запросы | Приоритет |
|---|---|---|
| `IDX_TDC_OPDAY (CCOPERATIONDAY)` (одиночный) | hasher.TURN_DOC_CUR, findRegistersWithActivityOnDay | средний |
| `IDX_TDC_TYPE (CCTYPEOPER)` (одиночный) | findAllPrimaryRegistersWithType50 | низкий (после prune этих записей <100/cluster) |
| `IDX_DB_OPDAY (CCOPERATIONDAY)` на DAY_BALANCES | hasher.DAY_BALANCES | средний |
| `IDX_REG_RECALC (CCBALANCERECALCDATE)` на REGISTER | SQL_REGISTERS_FOR_RECALC, SQL_ACTIVE_REGISTERS | **высокий** (используется в ночном пересчёте, > 100К регистров) |
| `IDX_CBRATE_CODE_DATE (CCCODE, CCDATE)` на CBRATE | SQL_CB_RATE — вызывается на каждый день в цепочке пересчёта | **высокий** |

---

## Что нельзя проверить в OSS-стенде

- **`/*+ QUERY_ENGINE('calcite') */`** хинты — Apache Ignite OSS 2.16 не содержит Calcite engine в стандартном image. Нужно подключить `ignite-calcite` в pom + `<CalciteQueryEngineConfiguration>` в XML конфиге (что уже сделано в `stmnt-ignite_precalc/ignite-local.xml`, но docker-compose использует vanilla image).
- **Reverse-index scans** — соответственно тоже только с Calcite.
- **Co-located JOIN'ы** — нужен multi-node cluster с одинаковыми affinity-настройками. Smoke single-node, эффект незаметен.

Для проверки последних двух пунктов нужен либо:
1. Реальный кластер Platform V Datagrid 17.6.3 + сборка stmnt-ignite_precalc, либо
2. Docker-image на базе `apacheignite/ignite` с дополнительно скопированным jar `ignite-calcite` в classpath + переопределённой `ignite-config.xml`.

---

## Версия Ignite — открытый вопрос

Сейчас:
- `consistency-service/pom.xml`: `org.apache.ignite:ignite-core:2.16.0` (Public Maven Central)
- `stmnt-ignite_precalc/pom.xml`: `com.sbt.ignite:ignite-core:${ignite.se.version}` где `ignite.se.version = 2.16.0`
- `docker-compose.yml`: `apacheignite/ignite:2.16.0`

Нужно (от пользователя):
- Точные Maven coordinates для Platform V Datagrid 17.6.3
- Docker image (если есть в Platform V registry)
- `settings.xml` для приватного Nexus, если нужен доступ

После подтверждения — единственное место где меняем:
1. property `<ignite.se.version>` в обоих pom
2. `groupId`/`artifactId` (если отличается от `com.sbt.ignite`)
3. image-tag в `docker-compose.yml`

Никакого code-change ожидаться не должно (API одинаковый, Platform V Datagrid — patch-релиз поверх Apache Ignite 2.x).
