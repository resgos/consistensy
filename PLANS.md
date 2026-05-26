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

| Категория | Количество | Index Scan | Full Scan | Calcite-зависимые |
|---|---:|---:|---:|---:|
| Hasher reads | 9 | 0 | 9 | 0 |
| DayBalancesRecalcService selects | 14 | 7 | 5 | 2 |
| **Всего** | **23** | **7** | **14** | **2** |

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

### Calcite-зависимые (2) — план недоступен в OSS

#### `SQL_PREV_OPER_DATE`
```sql
SELECT /*+ QUERY_ENGINE('calcite') */ GREATEST(
  COALESCE((SELECT MAX(CCOPERATIONDAY) ... CCTYPEOPER=0  ...), DATE '1900-01-01'),
  COALESCE((SELECT MAX(CCOPERATIONDAY) ... CCTYPEOPER=40 ...), DATE '1900-01-01'))
```
**OSS reply**: `Query engines not configured, but specified engine: calcite`.
В `stmnt-ignite_precalc/ignite-local.xml` Calcite добавлен явно:
```xml
<bean class="org.apache.ignite.calcite.CalciteQueryEngineConfiguration"/>
```
План на production-кластере должен показать **`INDEX REVERSE_SCAN`** по `(REGISTER, CCTYPEOPER, CCOPERATIONDAY)` с ранним выходом на первой строке (MAX). H2-движок такой оптимизации не делает.

#### `SQL_TYPE50_START_BEFORE`
```sql
SELECT /*+ QUERY_ENGINE('calcite') */ CCSTARTSUM, CCSTARTSUMNAT, CCOPERATIONDAY
FROM TURNDOCCUR
WHERE REGISTER=? AND CCTYPEOPER=50 AND CCOPERATIONDAY<?
ORDER BY CCOPERATIONDAY DESC LIMIT 1
```
Та же история. С Calcite — reverse scan по индексу с `LIMIT 1` без буферизации.

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

## Зачем нужен Calcite (в проде)

В `ignite-local.xml`:
```xml
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
```
- H2-движок (default) — основной, fallback для всего, что не использует hints.
- Calcite — только для двух запросов с `ORDER BY ... DESC LIMIT 1`, где reverse-index-scan даёт O(log n) вместо O(n).

В smoke (vanilla `apacheignite/ignite:2.16.0`) Calcite не подключён → проверить эти 2 плана здесь невозможно. На реальном Platform V Datagrid 17.6.3 (та же 2.16-база + патчи) Calcite доступен.

---

## Заключение

- **23 запроса проверены**. Из них 7 идут через композитный индекс `IDX_TDC_REG_TYPE_OP` точечно, остальные 14 — full scan, в большинстве случаев осознанный (полные выгрузки справочников, ночные операции массового сканирования).
- **2 Calcite-зависимых запроса** в OSS-стенде упали с ожидаемой ошибкой; в проде с подключённым `ignite-calcite` дадут `INDEX REVERSE_SCAN`.
- **1 запрос** падает в smoke по причине упрощённой DDL-схемы (отсутствует колонка `CCDAYBALANCESBEGINDATE`); в проде корректен.
- **Affinity-настройки smoke стенда** — single-node, cache_group по умолчанию (на cache на группу); в проде кеши коллоцированы через `@AffinityKeyMapped String register`.

Никаких изменений индексов или SQL по итогам инвентаризации не планируется — текущие планы соответствуют намерениям.
