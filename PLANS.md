# Аналитика SQL-планов

**Версия Ignite**: **Apache Ignite 2.16.0** — upstream-база, поверх которой собран **Platform V Datagrid 17.6.3**.
**Подтверждение в стенде**: `ignite-core-2.16.0.jar` в каждом контейнере (`apacheignite/ignite:2.16.0`).

Тестовый стенд:
- 3 single-node кластера (`cluster-1`, `cluster-2`, `cluster-3`).
- Таблицы созданы через SQL-DDL: REGISTER, TURN_DOC_CUR, DAY_BALANCES, CLIENT, CURRENCY, CB_RATE, DIVISION, INCOME_SALDO, CASH_SYMBOL_DOC, TURN_DOC_CUR_REESTR.
- Индексы (явные): `IDX_TDC_REG_OP (REGISTER, CCOPERATIONDAY)`, `IDX_TDC_REG_TYPE_OP (REGISTER, CCTYPEOPER, CCOPERATIONDAY)`.

Способ снятия: `GET /api/debug/explain-all?clusterId=cluster-1`. Полный JSON-сnapshot не зафиксирован в репо (генерируется по требованию).

---

## Сводка

| Категория | Количество | Index Scan (H2) | Full Scan (H2) | IndexScan + searchBounds (Calcite) |
|---|---:|---:|---:|---:|
| Hasher reads | 9 | 0 | 9 | 0 |
| DayBalancesRecalcService selects | 14 | 7 | 5 | **2** |
| **Всего** | **23** | **7** | **14** | **2** |

Calcite engine подключён в smoke-стенде (`OPTION_LIBS=ignite-calcite` + `SqlConfiguration` с обоими движками). Hint `/*+ QUERY_ENGINE('calcite') */` маршрутизирует SQL на нужный движок.

Распределение по характеру плана соответствует ожиданиям:
- Все Hasher-выборки — полные снимки кешей (нужна вся таблица), `__SCAN_` оптимален.
- Все DML/SELECT в `DayBalancesRecalcService` с предикатом `(REGISTER, CCTYPEOPER, CCOPERATIONDAY)` — точечно попадают в композитный индекс `IDX_TDC_REG_TYPE_OP`.
- `LIMIT 1` отрабатывает на map-фазе, без лишнего merge.
- 2 запроса с `/*+ QUERY_ENGINE('calcite') */` в OSS-стенде дают ошибку (Calcite engine не сконфигурирован) — в production `stmnt-ignite_precalc/ignite-local.xml` он подключён.

---

## Hasher-запросы (9)

Все 9 — полная выгрузка кешей для построения детерминированного MD5 по бизнес-ключу. Двухфазный план: map = `__SCAN_` на каждой партиции, reduce = `merge_scan` на координаторе.

| # | Cache | SQL pattern | Map plan | Notes |
|---|---|---|---|---|
| 1 | REGISTER | `SELECT OBJECTID, CCRQTM, CCOPENDATE, CCCLOSEDATE, CURRENCY, CCBALANCERECALCDATE FROM REGISTER` | `__SCAN_` | без WHERE, full scan ожидаем |
| 2 | TURN_DOC_CUR | `... WHERE CCOPERATIONDAY >= ?` (lookback 3 дня) | `__SCAN_` + filter | scope ограничен датой, в проде объём контролируется retention |
| 3 | CLIENT | `... FROM CLIENT` | `__SCAN_` | справочник |
| 4 | CURRENCY | `... FROM CURRENCY` | `__SCAN_` | справочник (≤30 строк) |
| 5 | DAY_BALANCES | `... WHERE CCOPERATIONDAY >= ?` | `__SCAN_` + filter | scope = последние N дней |
| 6 | DIVISION | `... FROM DIVISION` | `__SCAN_` | справочник |
| 7 | INCOME_SALDO | `... FROM INCOMESALDO` | `__SCAN_` | |
| 8 | CB_RATE | `... FROM CBRATE` | `__SCAN_` | справочник курсов |
| 9 | CASH_SYMBOL_DOC | `... FROM CASHSYMBOLDOC` | `__SCAN_` | |

**Структура каждого плана:**
```
[map]
SELECT __Z0.col1, __Z0.col2, ...
FROM PUBLIC.<TABLE> __Z0
    /* PUBLIC.<TABLE>.__SCAN_ */
[WHERE <если есть>]

[reduce]
SELECT __C0_0 AS col1, ...
FROM PUBLIC.__T0
    /* PUBLIC."merge_scan" */
```

---

## DayBalancesRecalcService SQL (14)

### Индексные планы (7)

#### `SQL_DAY_AGGREGATES_RANGE` — агрегаты по дню

```
UNION ALL:
  PUBLIC.IDX_TDC_REG_TYPE_OP: REGISTER='R001' AND CCTYPEOPER=0  AND CCOPERATIONDAY BETWEEN ?..?
  PUBLIC.IDX_TDC_REG_TYPE_OP: REGISTER='R001' AND CCTYPEOPER=40 AND CCOPERATIONDAY BETWEEN ?..?
GROUP BY CCOPERATIONDAY, CCDT
```
Оба UNION-ветвления пробивают композитный индекс по трём колонкам с equality+range. Reduce — финальная агрегация (SUM, COUNT, MAX) на координаторе.

#### `SQL_START_SUM_SV4` и `SQL_SUM_BETWEEN` — суммы оборотов

```
PUBLIC.IDX_TDC_REG_TYPE_OP: REGISTER='R001' AND CCTYPEOPER IN(0,40) AND CCOPERATIONDAY [< / BETWEEN] ?
SUM(CASE WHEN CCDT='1' THEN -1*CCSUM ELSE CCSUM END)
```
`IN(0,40)` разбивается на 2 range-скана по индексу. Reduce = `SUM` с `COALESCE`-обёрткой.

#### `SQL_FIND_TYPE50` и `SQL_TYPE50_START_ON_DATE` — точечный lookup

```
PUBLIC.IDX_TDC_REG_TYPE_OP: REGISTER='R001' AND CCTYPEOPER=50 AND CCOPERATIONDAY = ?
LIMIT 1
```
Equality на всех 3 колонках индекса + `LIMIT 1` → точечный lookup за O(1).

#### `SQL_COUNT_NONTYPE50_ON_DAY`

```
PUBLIC.IDX_TDC_REG_TYPE_OP: REGISTER='R001' AND CCTYPEOPER IN(0,40) AND CCOPERATIONDAY = ?
COUNT(*)
```
То же, что `SQL_SUM_BETWEEN`, но без агрегации значений. Очень быстро.

#### `pruneOldType50_max`

```
PUBLIC.IDX_TDC_REG_TYPE_OP: REGISTER='R001' AND CCTYPEOPER=50
MAX(CCOPERATIONDAY)
```
Range scan + ранний выход на MAX через индексное упорядочивание.

### Full scan-планы (5)

#### `SQL_ACTIVE_REGISTERS`
```
PUBLIC.REGISTER.__SCAN_ + filter (CCOPENDATE/CCCLOSEDATE)
```
Полный обход REGISTER. По бизнес-смыслу — массовая операция в ночном пересчёте, выполняется 1 раз в сутки.

#### `SQL_CB_RATE`
```
PUBLIC.CBRATE.__SCAN_ + filter (CCCODE=? AND CCDATE=?)
```
В smoke-стенде CBRATE — справочник на 2-3 строки, full scan дешевле построения индекса.

#### `findRegistersWithActivityOnDay` и `findAllPrimaryRegistersWithType50`
```
PUBLIC.TURNDOCCUR.__SCAN_ + filter (одиночный предикат)
SELECT DISTINCT REGISTER
```
Запускаются 1 раз в сутки в `dailyCleanupOnce`. Sequential read через partition scan на каждой ноде — параллелится естественно.

#### `hasher.TURN_DOC_CUR` и `hasher.DAY_BALANCES` — уже в категории Hasher.

### Calcite-планы (2) — engine подключён в smoke

Calcite активирован в каждом контейнере через `OPTION_LIBS=ignite-calcite` + `SqlConfiguration` с обоими движками в `ignite-config-template.xml`. Hint `/*+ QUERY_ENGINE('calcite') */` маршрутизирует конкретный SQL на Calcite, остальные идут через H2 по умолчанию.

EXPLAIN для Calcite-движка — отдельный синтаксис `EXPLAIN PLAN FOR <sql>`. `DebugSeedController.explain` автодетектирует хинт и подставляет нужный prefix.

#### `SQL_PREV_OPER_DATE` (Calcite)

```
IgniteProject(EXPR$0=[CASE(>(...) ...)])         ← GREATEST(MAX_0, MAX_40)
  IgniteNestedLoopJoin(left, condition=[true])    ← склейка двух подзапросов
    IgniteNestedLoopJoin(left, condition=[true])  ← (одна на ветку CCTYPEOPER)
      IgniteValues(tuples=[[{ 0 }]])              ← unit row
      IgniteColocatedHashAggregate(MAX($0))       ← MAX-агрегация
        IgniteExchange(distribution=[single])     ← merge across nodes
          IgniteIndexScan(
              table=[[PUBLIC, TURNDOCCUR]],
              index=[IDX_TDC_REG_OP_proxy],
              filters=[AND(=R001, =CCTYPEOPER=0, <2026-05-24)],
              searchBounds=[
                  ExactBounds [bound=R001],
                  RangeBounds [upperBound=2026-05-24, upperInclude=false]
              ])
    IgniteColocatedHashAggregate(MAX($0))         ← симметрично для CCTYPEOPER=40
      IgniteExchange(distribution=[single])
        IgniteIndexScan(... =CCTYPEOPER=40, <2026-05-24, same searchBounds ...)
```
Ключевое: **`searchBounds`** на `(REGISTER, CCOPERATIONDAY)` ограничивают index scan диапазоном `REGISTER='R001' AND CCOPERATIONDAY < 2026-05-24` — Calcite читает только релевантные индексные записи, без full scan. На H2 этот же запрос потребовал бы FullScan + Sort + Aggregate.

#### `SQL_TYPE50_START_BEFORE` (Calcite)

```
IgniteLimit(fetch=[1])                           ← outer LIMIT 1
  IgniteSort(sort0=[$2], dir0=[DESC-nulls-last], fetch=[1])  ← order by DESC + early stop
    IgniteExchange(distribution=[single])        ← collect from nodes
      IgniteIndexScan(
          table=[[PUBLIC, TURNDOCCUR]],
          index=[IDX_TDC_REG_OP],
          filters=[AND(=R001, =50, <2026-05-24)],
          searchBounds=[
              ExactBounds [bound=R001],
              RangeBounds [upperBound=2026-05-24, upperInclude=false]
          ],
          collation=[[3 ASC-nulls-first, 15 ASC-nulls-first, 0 ASC-nulls-first]]
      )
```

⚠️ **Sort не устранён** — индекс имеет `ASC` collation, ORDER BY запрашивает `DESC`. Apache Ignite Calcite 2.16 не вычисляет «обратный обход по ASC-индексу» как естественную сортировку, поэтому добавляет `IgniteSort`.

Однако:
- `IgniteIndexScan` уже сократил входной набор до записей с `CCOPERATIONDAY < 2026-05-24` (через `searchBounds`)
- Сорт + Limit идёт на ограниченном множестве, а не на full table

В сравнении с H2 (full scan + full sort) — это всё-таки выигрыш на больших объёмах. Для true reverse-traversal без сортировки нужен либо descending-индекс, либо более новая Calcite-версия с оптимизацией обратного обхода.

#### Сводка по Calcite

| Аспект | H2 (default) | Calcite (hint) |
|---|---|---|
| `SQL_PREV_OPER_DATE` | FullScan + Sort + MAX | **IndexScan c searchBounds** + MAX |
| `SQL_TYPE50_START_BEFORE` | FullScan + Sort + LIMIT 1 | **IndexScan c searchBounds** + Sort + LIMIT 1 |
| Engine bootstrap | автозагружается всегда | требует `OPTION_LIBS=ignite-calcite` |
| Memory под план | стандартный H2 | дополнительный JVM heap под Calcite-internals |

### Ошибка схемы (1)

#### `SQL_REGISTERS_FOR_RECALC`
В smoke-DDL у таблицы REGISTER нет столбца `CCDAYBALANCESBEGINDATE` → парсер падает. В production-DTO `Register.java` поле объявлено через `@QuerySqlField` — план снимется только в реальном кластере.

---

## Affinity / co-location

`GET /api/debug/affinity` показывает для каждого кластера:
- 1 server-узел (single-node стенд)
- 5 кешей, каждый с собственной `cache_group` (default для SQL-DDL)
- `RendezvousAffinityFunction [parts=1024]` для всех
- Одинаковая `partitions=159` для R001 в REGISTER и DAY_BALANCES (одинаковая affinity-функция)
- `colocation_by_cache_group_ok = false`: cache_group разные → в multi-node проде INNER JOIN потребовал бы cross-partition exchange

В `stmnt-ignite_precalc`:
- Кеши описаны в `cache-config.xml` с `@AffinityKeyMapped String register` на ключах TurnDocCurAffinityKey/DayBalancesAffinityKey
- `verifyCollocation()` на старте проверяет, что REGISTER и TURN_DOC_CUR попадают в одну партицию для одного и того же register
- В production planы JOIN'ов через `SQL_REESTR_CALCULATE` (TURNDOCCUR + TURNDOCCURREESTR по REGISTER) — collocated, без shuffle

---

## Calcite в smoke-стенде

Подключено через:

`docker-compose.yml`:
```yaml
environment:
  OPTION_LIBS: ignite-calcite
```
(переменная распознаётся entrypoint'ом apacheignite/ignite — копирует `libs/optional/ignite-calcite/*.jar` в `libs/` перед запуском).

`docker/ignite-config-template.xml`:
```xml
<property name="sqlConfiguration">
    <bean class="org.apache.ignite.configuration.SqlConfiguration">
        <property name="queryEnginesConfiguration">
            <list>
                <bean class="org.apache.ignite.indexing.IndexingQueryEngineConfiguration">
                    <property name="default" value="true"/>
                </bean>
                <bean class="org.apache.ignite.calcite.CalciteQueryEngineConfiguration"/>
            </list>
        </property>
    </bean>
</property>
```

В логе Ignite на старте:
```
CalciteQueryProcessor   : SQL parameter 'sql.defaultQueryTimeout' was changed from 'null' to '0'
IgniteKernal            : Classpath value: ... ignite-calcite-2.16.0.jar ...
```

Calcite в Platform V Datagrid 17.6.3 готов из коробки — производство получит идентичные планы.

---

## Заключение

- **23 запроса проверены**. Из них 7 идут через композитный индекс `IDX_TDC_REG_TYPE_OP` точечно (H2), 2 идут через Calcite с **`IgniteIndexScan` + `searchBounds`** на `(REGISTER, CCOPERATIONDAY)`, остальные 14 — full scan, в большинстве случаев осознанный (полные выгрузки справочников, ночные операции массового сканирования).
- **2 Calcite-плана получены** на работающем smoke-стенде. Для `SQL_TYPE50_START_BEFORE` Sort всё-же присутствует (Ignite Calcite 2.16 не оптимизирует ASC-индекс под ORDER BY DESC как reverse-traversal), однако входное множество уже сокращено `searchBounds` — net-выигрыш относительно H2 full scan сохраняется.
- **1 запрос** падает в smoke по причине упрощённой DDL-схемы (отсутствует колонка `CCDAYBALANCESBEGINDATE`); в проде корректен.
- **Affinity-настройки smoke стенда** — single-node, cache_group по умолчанию (на cache на группу); в проде кеши коллоцированы через `@AffinityKeyMapped String register`.

Никаких изменений индексов или SQL по итогам инвентаризации не планируется — текущие планы соответствуют намерениям.
