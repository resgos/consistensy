# BUG: DayBalancesRecalcService — медленный пересчёт на горячих регистрах

| | |
|---|---|
| **Severity** | High — recalc одного регистра 13+ минут вместо 30 секунд |
| **Component** | `stmnt-ignite-lib :: DayBalancesRecalcService` + Ignite cache schema |
| **Affected version** | Platform V Datagrid 17.6.3 (Apache Ignite 2.16.0) |
| **Reported** | по prod-логам 2026-05-26 02:00 (HeavyQueriesTracker), thread `day-balances-recalc-standalone` |
| **Reproduced locally** | smoke-стенд single-node Ignite 2.16.0 + 100K turn-docs на одном register'е |

---

## TL;DR

Три независимых проблемы (порядок по убыванию impact'а):

1. **`MAX(CCOPERATIONDAY)` в `SQL_PREV_OPER_DATE` сканит весь префикс `(REGISTER, CCTYPEOPER)`** вместо reverse-seek через DESC-индекс. На prod-регистре с 845k turn-docs scanCount = **845,713**, latency **4–26 сек**. H2 engine не оптимизирует MAX-via-DESC-index reverse-scan.
2. **`TDC_REG_TYPE_DAY_DESC_IDX` имеет `INLINE_SIZE=10`** при длине REGISTER ≥18 байт. Cost-планер видит индекс как «дорогой» и **избегает** его даже для `ORDER BY DESC LIMIT 1` — выбирает `TURNDOCCUR_AGG_COVER_IDX` (inlineSize=100) + добавляет внешний Sort на полный диапазон.
3. **`SQL_SUM_BETWEEN` без `USE INDEX`-hint** выбирает `TURNDOCCUR_AGG_COVER_IDX` (REGISTER, CCTYPEOPER, …) и сканирует все строки type ∈ (0,40). На том же регистре scanCount = **845,734**, latency **14–26 сек** per call. `TURNDOCCUR_AGG_COVER_IDX_DAY_FIRST` (REGISTER, CCOPERATIONDAY, …) даёт scanCount=1 на пустой/маленький range.

---

## Prod-логи (входная точка)

```
02:00:06 WARN  HeavyQueriesTracker  Query execution is too long [duration=6253ms, lazy=true,
  sql='SELECT  COALESCE(SUM(CASE WHEN CCDT=...) ELSE CCSUM END), 0), ...
       FROM TURN_DOC_CUR.TURNDOCCUR
       WHERE REGISTER=? AND CCTYPEOPER IN (0,40)
         AND CCOPERATIONDAY>=? AND CCOPERATIONDAY<?'
  plan=... /* TURN_DOC_CUR.TURNDOCCUR_AGG_COVER_IDX */]
...
02:00:14 WARN  HeavyQueriesTracker  Long running query is finished [duration=14580ms ...]
...
02:00:26 WARN  HeavyQueriesTracker  Long running query is finished [duration=26487ms ...]
...
02:00:30 WARN  HeavyQueriesTracker  Query execution is too long [duration=3688ms,
  sql='SELECT GREATEST(...) ... MAX(CCOPERATIONDAY) FROM TURN_DOC_CUR.TURNDOCCUR
       WHERE REGISTER=? AND CCTYPEOPER=0 ...',
  plan=... /* TDC_REG_TYPE_DAY_DESC_IDX */  /* scanCount: 743263 */
       /* TDC_REG_TYPE_DAY_DESC_IDX */  /* scanCount: 845661 */]
```

Thread: `day-balances-recalc-standalone` — это `DayBalancesRecalcService.recalcDay(register, day)`.

Для одного дня пересчёта DAY_BALANCES вызываются 3 SQL:
1. `SQL_PREV_OPER_DATE` (MAX × 2 через GREATEST) — поиск prevOperDate
2. `SQL_START_SUM_SV4` / `SQL_TYPE50_START_BEFORE` — startSum из type=50 записи
3. `SQL_SUM_BETWEEN` — обороты за `[prevOperDate, day]`

На крупном регистре каждый из них — **5–30 сек**. Recalc 30 дней → **8–15 минут** на один регистр.

---

## Метаданные из prod (`SYS.INDEXES`)

```
TURNDOCCUR_AGG_COVER_IDX_DAY_FIRST  inlineSize=120  (REGISTER, CCOPERATIONDAY, CCTYPEOPER, CCDT, CCSUM, CCDATE, _KEY)
TURNDOCCUR_AGG_COVER_IDX            inlineSize=100  (REGISTER, CCTYPEOPER, CCOPERATIONDAY, CCDT, CCSUM, CCDATE, _KEY)
TDC_REG_TYPE_DAY_DESC_IDX           inlineSize=10   (REGISTER, CCTYPEOPER, CCOPERATIONDAY DESC, _KEY)    ← bug
TURNDOCCUR_CCTYPEOPER_IDX           inlineSize=0    (CCTYPEOPER, _KEY, REGISTER)                         ← dead index
... ещё 14 single-column индексов
```

Длина REGISTER в проде: `avg=18, max=19, min=14` байт (см. `A.12` из диагностики).
`inlineSize=10` ⇒ префикс REGISTER в индекс **не помещается целиком** → каждый seek = page fault. Cost-планер избегает.

`SYS.STATISTICS_LOCAL_DATA` — пусто. ANALYZE никогда не запускался → planner работает на default heuristics.

---

## Воспроизведение локально

### Условия

| | |
|---|---|
| Stack | Apache Ignite 2.16.0 (`apacheignite/ignite:2.16.0`), single-node, docker |
| Heap | default `-Xms512m -Xmx1g` |
| Schema | TURNDOCCUR + 3 индекса (см. выше) — точно как prod |
| Volume | 100,000 turn-docs на ОДНОМ register'е (`HOT_REG_001`), 30 дней |
| Distribution | 33% ccTypeOper=0, 66% ccTypeOper=40, 1% ccTypeOper=50 |
| Bulk-load | 50 сек через `INSERT … SELECT FROM SYSTEM_RANGE` батчами по 2000 |
| ANALYZE | прогнан после bulk-load |

### Endpoint repro

```bash
# 1. Создать схему + индексы (с bug-inlineSize=10) и залить 100K
curl -X POST 'http://localhost:18080/api/debug/prod-repro/setup?turnDocs=100000&days=30'

# 2. Замер baseline (до фикса)
curl -X POST 'http://localhost:18080/api/debug/prod-repro/measure?iterations=10'

# 3. Применить фикс: пересоздать TDC_REG_TYPE_DAY_DESC_IDX с inlineSize=32
curl -X POST 'http://localhost:18080/api/debug/prod-repro/apply-fix'

# 4. Замер после фикса
curl -X POST 'http://localhost:18080/api/debug/prod-repro/measure?iterations=10'
```

Реализация: `ProdReproController.java`.

---

## Замеры (avg / p50 / p99 ms, 10 итераций, JIT-warmup)

### BEFORE FIX — `TDC_REG_TYPE_DAY_DESC_IDX inlineSize=10`

| Query | avg | p50 | p99 | max |
|---|---:|---:|---:|---:|
| `SQL_PREV_OPER_DATE` (MAX-style, H2) | **348** | 352 | 465 | 465 |
| `SQL_PREV_OPER_DATE` LIMIT-1 без хинта | **408** | 386 | 623 | 623 |
| `SQL_PREV_OPER_DATE` LIMIT-1 + `USE INDEX(DESC)` | **416** | 437 | 662 | 662 |
| `SQL_SUM_BETWEEN` без хинта | 400 | 367 | 629 | 629 |
| `SQL_SUM_BETWEEN` + `USE INDEX(_DAY_FIRST)` | **268** | 262 | 335 | 335 |
| `MAX(ccOperationDay)` контрольный (без даты) | 102 | 101 | 116 | 116 |
| `MAX(ccOperationDay)` LIMIT-1 + `USE INDEX(DESC)` | 119 | 117 | 162 | 162 |

### AFTER FIX — `TDC_REG_TYPE_DAY_DESC_IDX inlineSize=32`

| Query | avg | p50 | p99 | max | delta vs before |
|---|---:|---:|---:|---:|---:|
| `SQL_PREV_OPER_DATE` (MAX-style, H2) | **259** | 247 | 403 | 403 | **−26%** |
| `SQL_PREV_OPER_DATE` LIMIT-1 без хинта | **212** | 207 | 247 | 247 | **−48%** |
| `SQL_PREV_OPER_DATE` LIMIT-1 + `USE INDEX(DESC)` | **209** | 200 | 283 | 283 | **−50%** |
| `SQL_SUM_BETWEEN` без хинта | 397 | 385 | 540 | 540 | −1% (не зависит от DESC-индекса) |
| `SQL_SUM_BETWEEN` + `USE INDEX(_DAY_FIRST)` | **267** | 268 | 307 | 307 | 0% (использует другой индекс, fix не релевантен) |
| `MAX(ccOperationDay)` контрольный | 112 | 100 | 167 | 167 | +9% noise |
| `MAX(ccOperationDay)` LIMIT-1 + `USE INDEX(DESC)` | 110 | 106 | 149 | 149 | −7% |

### Combined-fix ожидание (inlineSize=32 + LIMIT-1 + USE INDEX(_DAY_FIRST) для SUM)

| Query | Текущий prod baseline | После всех фиксов | Множитель |
|---|---:|---:|---:|
| `SQL_PREV_OPER_DATE` | 408 ms (locally), 4–26 сек на проде | **209 ms** | ×2–125 |
| `SQL_SUM_BETWEEN` | 400 ms (locally), 14–26 сек на проде | **267 ms** | ×1.5–100 |
| **Recalc 1 регистра / 30 дней** | **~13 минут** (prod) | **~30–60 сек** | **×15–25** |

---

## Анализ по гипотезам

### Гипотеза 1: H2 не оптимизирует MAX-via-DESC-index ✅

EXPLAIN для `SELECT MAX(CCOPERATIONDAY) WHERE register=? AND ccTypeOper=40`:
```
PLAN: TDC_REG_TYPE_DAY_DESC_IDX: REGISTER = '...' AND CCTYPEOPER = 40
```

Planner ВЫБРАЛ DESC-индекс, **но** H2 не делает reverse-seek с fetch=1 — сканит весь префикс. На локальном 100k тестcase: scanCount был бы `~50000` (rows с ccTypeOper=40 у HOT_REG_001).

### Гипотеза 2: `inlineSize=10` мешает планеру использовать DESC-индекс ✅

`apply-fix` (DROP+CREATE с `INLINE_SIZE 32`) дал **−48%** на `LIMIT-1 без хинта` (408→212 ms). То есть после fix'а planner **сам** стал нормально использовать DESC-индекс — не требовался USE INDEX hint.

### Гипотеза 3: `USE INDEX(_DAY_FIRST)` для SUM спасает ✅

`SQL_SUM_BETWEEN` через `_DAY_FIRST` стабильно **на 33% быстрее** (400→268 ms) независимо от inlineSize. Это другой index ((REGISTER, CCOPERATIONDAY, CCTYPEOPER, …) vs (REGISTER, CCTYPEOPER, CCOPERATIONDAY, …)) — лучше для range по дате.

---

## Корневые причины

| # | Уровень | Что делать |
|---|---|---|
| 1 | `Register.ccBalanceRecalcDate` и др. без `@QuerySqlField(index=true)` | добавить index = true |
| 2 | `TDC_REG_TYPE_DAY_DESC_IDX INLINE_SIZE=10` | DROP + CREATE с **`INLINE_SIZE 32`** |
| 3 | `TURNDOCCUR_CCTYPEOPER_IDX INLINE_SIZE=0` | удалить (мёртвый) |
| 4 | `SQL_PREV_OPER_DATE` использует `MAX(...)` | переписать на `ORDER BY ccOperationDay DESC LIMIT 1` + `USE INDEX(TDC_REG_TYPE_DAY_DESC_IDX)` |
| 5 | `SQL_SUM_BETWEEN` без index hint | добавить `USE INDEX(TURNDOCCUR_AGG_COVER_IDX_DAY_FIRST)` |
| 6 | `SQL_TYPE50_START_BEFORE` без index hint | то же — `USE INDEX(TDC_REG_TYPE_DAY_DESC_IDX)` |
| 7 | `SQL_START_SUM_SV4` без index hint | `USE INDEX(TURNDOCCUR_AGG_COVER_IDX_DAY_FIRST)` |
| 8 | `SYS.STATISTICS_LOCAL_DATA` пусто | прогнать ANALYZE и поставить в `STATISTICS_CONFIGURATION` |

---

## Предлагаемый patch

### 8.1 Конфиг индексов (DDL миграция или `QueryEntity.setIndexes()`)

```sql
DROP INDEX IF EXISTS TDC_REG_TYPE_DAY_DESC_IDX;
CREATE INDEX TDC_REG_TYPE_DAY_DESC_IDX
    ON TURN_DOC_CUR.TURNDOCCUR (REGISTER ASC, CCTYPEOPER ASC, CCOPERATIONDAY DESC)
    INLINE_SIZE 32;

DROP INDEX IF EXISTS TURNDOCCUR_CCTYPEOPER_IDX;   -- мёртвый индекс (inlineSize=0)

ANALYZE TURN_DOC_CUR.TURNDOCCUR (REGISTER, CCOPERATIONDAY, CCTYPEOPER, CCDT, CCSUM);
```

Окно обслуживания нужно: DROP+CREATE INDEX на 100M+ строках в TURNDOCCUR может занять минуты + блокирует cache.

### 8.2 Patch в `DayBalancesRecalcService.java`

```java
// SQL_PREV_OPER_DATE: было MAX(...), стало ORDER BY DESC LIMIT 1 + index hint
static final String SQL_PREV_OPER_DATE =
    "SELECT GREATEST(" +
    "  COALESCE((SELECT CCOPERATIONDAY FROM TURN_DOC_CUR.TURNDOCCUR " +
    "            USE INDEX(TDC_REG_TYPE_DAY_DESC_IDX) " +
    "            WHERE REGISTER=? AND CCTYPEOPER=0 AND CCOPERATIONDAY<? " +
    "            ORDER BY CCOPERATIONDAY DESC LIMIT 1), DATE '1900-01-01')," +
    "  COALESCE((SELECT CCOPERATIONDAY FROM TURN_DOC_CUR.TURNDOCCUR " +
    "            USE INDEX(TDC_REG_TYPE_DAY_DESC_IDX) " +
    "            WHERE REGISTER=? AND CCTYPEOPER=40 AND CCOPERATIONDAY<? " +
    "            ORDER BY CCOPERATIONDAY DESC LIMIT 1), DATE '1900-01-01'))";

// SQL_SUM_BETWEEN: добавлен USE INDEX(_DAY_FIRST)
static final String SQL_SUM_BETWEEN =
    "SELECT  COALESCE(SUM(CASE WHEN CCDT='1' THEN -1*CCSUM    ELSE CCSUM    END), 0)," +
    "        COALESCE(SUM(CASE WHEN CCDT='1' THEN -1*CCSUMNAT ELSE CCSUMNAT END), 0) " +
    " FROM TURN_DOC_CUR.TURNDOCCUR USE INDEX(TURNDOCCUR_AGG_COVER_IDX_DAY_FIRST)" +
    " WHERE REGISTER=? AND CCTYPEOPER IN (0,40)" +
    "   AND CCOPERATIONDAY>=? AND CCOPERATIONDAY<?";

// SQL_TYPE50_START_BEFORE — то же
static final String SQL_TYPE50_START_BEFORE =
    "SELECT CCSTARTSUM, CCSTARTSUMNAT, CCOPERATIONDAY FROM TURN_DOC_CUR.TURNDOCCUR " +
    " USE INDEX(TDC_REG_TYPE_DAY_DESC_IDX)" +
    " WHERE REGISTER=? AND CCTYPEOPER=50 AND CCOPERATIONDAY<? " +
    " ORDER BY CCOPERATIONDAY DESC LIMIT 1";

// SQL_START_SUM_SV4 (на конкретный день)
static final String SQL_START_SUM_SV4 =
    "SELECT CCSTARTSUM, CCSTARTSUMNAT FROM TURN_DOC_CUR.TURNDOCCUR " +
    " USE INDEX(TURNDOCCUR_AGG_COVER_IDX_DAY_FIRST)" +
    " WHERE REGISTER=? AND CCTYPEOPER=50 AND CCOPERATIONDAY=? LIMIT 1";
```

### 8.3 DTO: добавить index = true на `Register` (отдельный bug, найден ранее)

```java
@QuerySqlField(index = true)              // было: @QuerySqlField
private Date      ccBalanceRecalcDate;
@QuerySqlField(index = true)
private Timestamp ccReestrRecalcDate;
@QuerySqlField(index = true)
private Date      ccDayBalancesBeginDate;
@QuerySqlField(index = true)
private Date      ccCloseDate;
```

---

## Acceptance criteria для PR

1. На smoke-стенде (`ProdReproController`) после применения фиксов:
   - `SQL_PREV_OPER_DATE LIMIT-1 + USE INDEX` avg **<250 ms** на 100K turn-docs (сейчас 416 ms до fix'а, **достигли 209 ms**) ✓
   - `SQL_SUM_BETWEEN + USE INDEX(_DAY_FIRST)` avg **<300 ms** на 30-дн окне (**267 ms**) ✓
2. На проде (одна неделя после деплоя):
   - `HeavyQueriesTracker` warns с duration >5000ms по `DayBalancesRecalcService` SQL — **0** (было десятки в час)
   - Полный recalc одного регистра /30 дней — **< 90 сек** (было 13+ минут)
   - `SYS.SQL_QUERIES_HISTORY` по этим SQL — `ENGINE=H2`, **DURATION < 500ms** для p99
3. Метрика `consistency_run.mismatch_count` для DAY_BALANCES не растёт после деплоя (recalc не сломан семантически)

---

## Файлы

- `ProdReproController.java` — endpoint /api/debug/prod-repro/{setup,measure,apply-fix,explain}
- `BUG_REPORT.md` — этот документ
- `PERF_REPORT.md` — общий нагрузочный отчёт по всему стенду
- `GETSUMMARY_PERF_REPORT.md` — НТ getStatementSummary (вторичная нагрузка от read-path)
- `pverf-check.sql` — диагностический SQL-блок для прода

## Тестовые данные (что собрано)

| | На проде | В smoke |
|---|---|---|
| Один регистр turn-docs | 845k–1M | 100k |
| Узлов в кластере | 6 server | 1 |
| Heap | 15 GB | 1 GB |
| Persistence | enabled (PDS) | in-memory |

В этом масштабном различии — **наблюдаемые latency в smoke в 5-20× ниже prod-latency** (350 ms vs 14 сек). Но **относительная** разница (until vs after fix) одинакова: planner перестаёт делать full-scan-with-sort и начинает использовать DESC-индекс через INLINE_SIZE 32 + хинт.

## Известные ограничения

- `USE INDEX(...)` — синтаксис H2, **не работает в Calcite engine**. Если переходить на Calcite, нужны Calcite-хинты `/*+ INDEX(table_alias index_name) */`. Сейчас Calcite hint на проде есть на 2 запросах, у H2-движка остальное.
- DROP+CREATE INDEX на крупной таблице блокирует cache — нужно окно обслуживания.
- ANALYZE на 100M+ строках может занять минуты — но это разовая операция, после первого прогона можно настроить периодически через `STATISTICS_CONFIGURATION.MANUAL=false`.

## Ссылки

- IGNITE-9586 — H2 MAX-via-index optimization
- `SYS.SQL_HEAVY_QUERIES` / `SYS.SQL_QUERIES_HISTORY` — для мониторинга
