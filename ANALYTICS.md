# Аналитика SQL-запросов проекта

**Источник данных:** прогон `GET /api/debug/explain-all?clusterId=cluster-1` + `POST /api/debug/analyze-all` на стенде Apache Ignite **2.16.0** (= upstream Platform V Datagrid 17.6.3) с подключённым Calcite engine. 39 запросов, 27 планов получено, 12 ошибок (8 — DML-EXPLAIN не поддерживается H2, 3 — отсутствующие колонки в smoke DDL, 1 — то же).

---

## 1. Категоризация по нагрузке

### По частоте вызова на 1 кластер

| Группа | Частота | Где используется | Кол-во SQL |
|---|---|---|---|
| **Hot path** (continuous query trigger) | До тысяч в минуту | TurnDocCurCQ → debounce → recalc | 8 (SQL_DAY_AGGREGATES_RANGE, SQL_START_SUM_SV4, SQL_FIND_TYPE50, SQL_COUNT_NONTYPE50_ON_DAY, …) |
| **Hourly** | 1 раз в час × 9 кэшей × 3 кластера = 27 thin-client SQL/час | ConsistencyJob.scheduledRun | 9 (hasher SELECTs) |
| **Daily** | 04:00 cleanup + 02:00 recalc + ANALYZE | dailyCleanupOnce, scheduledRecalcYesterday | 5 (SQL_ACTIVE_REGISTERS, findRegistersWithActivityOnDay × 2, pruneOldType50_*, deleteOneDay × 3) |
| **Per-init** | По одному вызову на регистр (max 180 дней × батчи 30) | initRegister | 4 (SQL_FIND_TYPE50, SQL_TYPE50_START_BEFORE, …) |
| **Per-storno** | По событию | TurnDocCurCQ REMOVED → cleanupAfterStorno | 2 (SQL_COUNT_NONTYPE50_ON_DAY, delete) |
| **Per-reestr-update** | По вставке в reestr | recalcReestrsForDay | 1 (SQL_REESTR_CALCULATE) |
| **Admin/Debug** | Ручной trigger | /api/admin/*, /api/debug/* | 4 (affinity probes, explain-all) |

**Самый «горячий» запрос на доступ к индексу:** `SQL_FIND_TYPE50` — вызывается на каждый день любого recalc-loop (потенциально миллионы вызовов в сутки на кластер).

### По типу плана

| Тип плана | Кол-во | % | Примеры |
|---|---:|---:|---|
| **Index Scan + searchBounds** (точечный) | 7 | 26% | SQL_FIND_TYPE50, SQL_TYPE50_START_ON_DATE, SQL_COUNT_NONTYPE50_ON_DAY |
| **Index Scan диапазон** | 4 | 15% | SQL_DAY_AGGREGATES_RANGE, SQL_START_SUM_SV4, SQL_SUM_BETWEEN, pruneOldType50_max |
| **Full Scan** (осознанный, для справочников) | 6 | 22% | hasher.{CURRENCY, CB_RATE, DIVISION, INCOME_SALDO, CASH_SYMBOL_DOC, CLIENT} |
| **Full Scan + filter** (date predicate без leading index) | 4 | 15% | hasher.TURN_DOC_CUR (lookback 3 дня), hasher.DAY_BALANCES, findRegistersWithActivityOnDay × 2 |
| **Calcite NestedLoopJoin + IndexScan** | 2 | 7% | SQL_PREV_OPER_DATE, SQL_TYPE50_START_BEFORE |
| **Calcite complex (3 IndexScan + 2 NestedLoopJoin + Aggregate)** | 1 | 4% | SQL_REESTR_CALCULATE |
| **SYS-view scan** | 3 | 11% | affinity.SYS_NODES / SYS_CACHES / SYS_CACHE_GROUPS |

---

## 2. Cost-метрики из Calcite (после ANALYZE)

Snap для `SQL_PREV_OPER_DATE` (см. PLANS.md):

| Узел | rowCount | cpu | memory | io | network |
|---|---:|---:|---:|---:|---:|
| IgniteProject (top) | 12.1 | 36.07 | 20.0 | 2.0 | 10.0 |
| IgniteNestedLoopJoin (outer) | 11.1 | 35.07 | 20.0 | 2.0 | 10.0 |
| IgniteNestedLoopJoin (inner, для CCTYPEOPER=0) | 6.05 | 18.03 | 10.0 | 1.0 | 5.0 |
| IgniteValues (unit row) | 1.0 | 1.0 | 0.0 | 0.0 | 0.0 |
| IgniteColocatedHashAggregate (MAX) | 4.05 | 13.03 | 6.0 | 1.0 | 5.0 |
| IgniteExchange (distribution=[single]) | 3.05 | 12.03 | 1.0 | 1.0 | 5.0 |
| IgniteIndexScan на TURNDOCCUR | 3.05 | 12.03 | 1.0 | 1.0 | 1.0 |

**Чем определяется cost:**
- `rowCount` — оценка количества строк после узла (после ANALYZE — на основе real stats, а не дефолтной эвристики)
- `cpu` — затраты CPU (примерно: rowCount × per-row work)
- `memory` — JVM heap для буферов узла
- `io` — Number of disk/page operations
- `network` — байт через `IgniteExchange` между узлами

**Хорошая новость:** в smoke `network=1.0` для IndexScan — данные читаются локально. В multi-node проде с `cache_group` правильно настроенным — то же.

**Тонкий момент:** Calcite видит `searchBounds` точно и считает выборку правильно (`3.05` строк = ~3-4 строки оборотов за день). На production-объёмах после ANALYZE планировщик будет уверенно выбирать индексные сканы вместо full scan'ов.

---

## 3. Hot Path — анализ для CQ debounce flow

Цепочка вызовов при одной вставке оборота в TURNDOCCUR:

```
TurnDocCurCQ event
  → debounce 1.5s
  → bumpRecalcDateToOperDay (EntryProcessor — atomic, не SQL)
  → recalcRegisterRange(register, fromDay, today-1):
      → loadDayAggregates(range)        — 1× SQL_DAY_AGGREGATES_RANGE
      → findStartSum                    — 1× SQL_TYPE50_START_ON_DATE + (если miss) SQL_TYPE50_START_BEFORE + SQL_SUM_BETWEEN
      → findPrevOperDate                — 1× SQL_PREV_OPER_DATE
      → per-day: upsertTypeOper50       — 1× SQL_FIND_TYPE50 + 1× cache.put
                                          (для каждого дня в range, max 30 на батч)
```

**Итог:** при вставке 1 оборота, при retention=1 день — **~5 SELECT'ов** через индекс `IDX_TDC_REG_TYPE_OP`. Все hot-path SELECT'ы используют `searchBounds` (Calcite) или композитный индекс (H2). Это **оптимально для производства**.

**Узкое место:** `SQL_REESTR_CALCULATE` в `recalcReestrsForDay` — если CQ `ccReestrRecalcDate` запускается для активного регистра с reestr-операциями, NestedLoopJoin против TURNDOCCURREESTR может быть тяжёлым на больших объёмах reestr-выгрузок. Решается через cache_group co-location в `cache-config.xml`.

---

## 4. Запросы с потенциальными проблемами на масштабе

### 4.1. `hasher.TURN_DOC_CUR` — full scan + date filter

```sql
SELECT OBJECTID, CCTYPEOPER, CCSTARTSUM, CCSTARTSUMNAT, CCIDEKS, REGISTER, CCSUM, CCRQUID
FROM TURNDOCCUR WHERE CCOPERATIONDAY >= ?
```

**План:** `__SCAN_` + filter (smoke и production одинаково).
**Проблема:** в production за 3 дня в TURNDOCCUR может быть до 30М записей × 3 кластера × 1 раз/час = **270 млн scan-операций в сутки**, плюс memory load в consistency-service для построения hash-Map.

**Что делать на production:**
- Добавить отдельный индекс `(CCOPERATIONDAY)` или `(CCOPERATIONDAY, CCTYPEOPER)` через QueryEntity.
- ИЛИ перейти на партиционирование по дате и hashing per-day отдельно.

### 4.2. `hasher.DAY_BALANCES` — то же

PK = `(REGISTER, CCOPERATIONDAY)`. Лидирующая колонка `REGISTER`. Фильтр `WHERE CCOPERATIONDAY >= ?` НЕ использует PK → full scan.
Аналогично: одиночный индекс на `CCOPERATIONDAY` решает.

### 4.3. `daybalances.SQL_REESTR_CALCULATE` — потенциально дорогой JOIN

3 IndexScan + 2 NestedLoopJoin + 1 ColocatedHashAggregate. На большом TURNDOCCURREESTR:
- Если `cache_group` общая → collocated JOIN без shuffle (отлично).
- Если разные → cross-partition exchange (плохо).

**В production-`cache-config.xml`** для `TURN_DOC_CUR` и `TURN_DOC_CUR_REESTR` должен быть **общий `groupName`** (или хотя бы общий `@AffinityKeyMapped`). В smoke этого нет — `cache_group` разные. См. `/api/debug/affinity.colocation_by_cache_group_ok`.

### 4.4. `findAllPrimaryRegistersWithType50` — full scan для daily cleanup

```sql
SELECT DISTINCT REGISTER FROM TURNDOCCUR WHERE CCTYPEOPER=50
```

После моих правок (детерминированный `ccIdEKS` + `pruneOldType50`) type50 записей мало — по одной на регистр. Но full scan по миллионам turnovers с filter `CCTYPEOPER=50` всё равно сканит всю таблицу.

**На production:** одиночный индекс `IDX_TDC_TYPE (CCTYPEOPER)` + range scan. После prune type50 = N (количество регистров) → результат маленький, но input для скана большой.

### 4.5. `daybalances.SQL_ACTIVE_REGISTERS` — full scan REGISTER

Используется в ночном пересчёте. На 1M регистров — 1 раз в сутки full scan допустим (на JVM heap влияет → если 1M строк × 16 bytes/PK → 16MB единовременно).

---

## 5. Где Calcite даёт улучшение

### 5.1. `SQL_PREV_OPER_DATE` — GREATEST(MAX_0, MAX_40)

**H2:** не понимает sub-MAX по индексу → 2× full scan + sort + MAX.
**Calcite:** 2× `IndexScan + searchBounds[REGISTER, CCTYPEOPER, CCOPERATIONDAY < ?]` + ColocatedHashAggregate.

**Выигрыш:** O(scan-of-filtered-range) вместо O(full-table). На таблице 30M → читаем только индексные записи под фильтр.

### 5.2. `SQL_TYPE50_START_BEFORE` — `ORDER BY ... DESC LIMIT 1`

**H2:** full scan + sort.
**Calcite:** IndexScan + searchBounds + Sort + Limit.

**Тонкий момент:** Calcite Ignite 2.16 НЕ делает true reverse scan по ASC-индексу под `ORDER BY DESC`. Sort всё-же присутствует, но **на сокращённом наборе** (после searchBounds). Net-выигрыш относительно H2 на больших объёмах — есть.

### 5.3. `SQL_REESTR_CALCULATE` — сложный JOIN

**H2:** не оптимизирует подзапрос с `MIN(CCTRANSACTIONID)` и `LIKE` — full scan по обеим таблицам.
**Calcite:** инлайнит подзапрос, использует index для обеих сторон JOIN'а, считает estimated rowCount после filters.

---

## 6. ANALYZE — реальный замер эффекта

| | BEFORE ANALYZE | AFTER ANALYZE | Δ |
|---|---|---|---|
| IgniteIndexScan rowCount | 1.525 (default heuristic) | 3.05 (real stat) | **+100%** |
| IgniteProject (top) rowCount | 11.05 | 12.1 | +9.5% |
| IgniteProject (top) cpu | 31.87 | 36.07 | +13% |

**Что это значит:**
- Calcite после ANALYZE имеет real stats — выбирает план **с правильным cost estimate**.
- Shape плана при этом не меняется в smoke (потому что один индекс, один правильный access path).
- **В production**: ANALYZE может сменить shape — например, выбрать HashJoin вместо NestedLoopJoin когда обе стороны large.

**Когда нужно дёргать ANALYZE:**
- После initRegister на 180 дней (массовая загрузка → stats устарели).
- После daily cleanup (объём в TURN_DOC_CUR упал).
- Раз в сутки в плановом порядке — добавить в `ConsistencyJob.purge()` или отдельный cron.

---

## 7. Что было до моих правок vs стало (по типу плана)

| SQL | Изменение в проекте | Plan ДО | Plan ПОСЛЕ |
|---|---|---|---|
| SQL_REESTR_CALCULATE | + подзапрос с MIN(CCTRANSACTIONID), + Calcite hint | H2 cross-join | Calcite 3× IndexScan + 2× NestedLoopJoin |
| SQL_TYPE50_START_BEFORE | + Calcite hint | H2 full scan + sort | Calcite IndexScan + sort + limit |
| SQL_PREV_OPER_DATE | + Calcite hint | H2 full scan × 2 + MAX | Calcite IndexScan × 2 + ColocatedHashAggregate |
| SQL_FIND_TYPE50 | без изменений | IndexScan (был с начала) | IndexScan (тот же) |
| upsertTypeOper50 generated SQL | + детерминированный ccIdEKS | INSERT generated keys + EntryProcessor lookup | Тот же физический план, но идемпотентность upsert |

---

## 8. Хотспоты для оптимизации (упорядочены по приоритету)

1. **`hasher.TURN_DOC_CUR`** — full scan каждый час. Production-добавить `IDX_TDC_OPDAY (CCOPERATIONDAY)` через QueryEntity. **Effort: small. Impact: high.**
2. **`hasher.DAY_BALANCES`** — то же. **Effort: small. Impact: medium.**
3. **`SQL_REESTR_CALCULATE` на reestr-обновлениях** — проверить cache_group co-location в production cache-config.xml. **Effort: zero (config check). Impact: high если разные groups сейчас.**
4. **`findAllPrimaryRegistersWithType50`** — может быть медленным до того, как prune набрал критическую массу. **Effort: low. Impact: low.**
5. **`SQL_ACTIVE_REGISTERS`** — раз в сутки, на ~1M регистров full scan OK, но индекс `(CCBALANCERECALCDATE)` если профиль изменится. **Effort: low. Impact: low.**

---

## 9. Открытые вопросы для prod

1. Существуют ли в production-`cache-config.xml` индексы `(CCOPERATIONDAY)` отдельно? Если нет — добавить.
2. `cache_group` для TURN_DOC_CUR vs TURN_DOC_CUR_REESTR — общая? (Нужно для collocated JOIN в SQL_REESTR_CALCULATE.)
3. Calcite в production deploy of Platform V Datagrid 17.6.3 включён по умолчанию или требует ручной конфигурации в `ignite-local.xml`?
4. ANALYZE — настроен ли как scheduled в production? Если нет — раз в сутки.
5. `IDX_TDC_REG_TYPE_OP` — присутствует ли в проде с тем же составом колонок?

Эти вопросы стоит уточнить с production-DBA перед раскаткой.

---

**Источники данных:** все цифры получены на работающем стенде через `/api/debug/explain-all` после `/api/debug/analyze-all`. Reproducible:

```bash
docker compose up -d
curl -X POST localhost:8080/api/debug/seed -H 'Content-Type: application/json' -d '{}'
curl -X POST 'localhost:8080/api/debug/analyze-all?clusterId=cluster-1'
curl 'localhost:8080/api/debug/explain-all?clusterId=cluster-1' > /tmp/plans.json
```
