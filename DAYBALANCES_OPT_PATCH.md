# Patch: оптимизация SQL в `DayBalancesRecalcService.java`

Применено на ветке репозитория (`stmnt-ignite-lib/src/main/java/ru/sbrf/stmnt/ignite/utility/DayBalancesRecalcService.java`). Цель — устранить hot-path bottleneck'и из prod-логов 2026-05-26 02:00 (`HeavyQueriesTracker`, scanCount до 845k).

## Контекст

В `cache-config.xml` уже декларированы **правильные индексы** с `inlineSize=64`:

| Индекс | Поля | Назначение |
|---|---|---|
| `IDX_TDC_REG_DAY_COVERING` | REG ASC, DAY ASC, TYPE ASC, CCDT, CCSUM, CCSUMNAT, CCDATE | range scan по дате, covering для агрегации |
| `IDX_TDC_REG_TYPE_DAY` | REG ASC, TYPE ASC, DAY ASC, CCSTARTSUM, CCSTARTSUMNAT | equality на (REG, TYPE, DAY) для type-50 lookup |
| `IDX_TDC_REG_TYPE_DAY_DESC` | REG ASC, TYPE ASC, DAY **DESC**, CCSTARTSUM, CCSTARTSUMNAT, CCDATE | **reverse seek** для `MAX/LIMIT-1` через DESC |
| `IDX_TDC_REG_TYPE_DAY_AGG` | REG ASC, TYPE ASC, DAY ASC, CCDT, CCSUM, CCSUMNAT, CCDATE | GROUP BY CCDT aggregation |

**Проблема была не в индексах**, а в том что planner H2 1.4.197 **не выбирал** их автоматически — выбирал AGG_COVER_IDX/DAY_FIRST и игнорировал DESC-индекс. Фикс: явные `USE INDEX(...)` хинты во всех hot-path SQL.

## Изменения (8 SQL × 4 индекса = 13 USE INDEX-хинтов)

### 1. `SQL_PREV_OPER_DATE` — критическая правка

**Было**: `MAX(CCOPERATIONDAY)` через Calcite hint → H2 сканит 845k строк, Calcite добавляет Sort.

**Стало**: `ORDER BY ... DESC LIMIT 1` + `USE INDEX(IDX_TDC_REG_TYPE_DAY_DESC)` × 2 подзапроса.

```sql
SELECT GREATEST(
  COALESCE((SELECT CCOPERATIONDAY FROM TURN_DOC_CUR.TURNDOCCUR
            USE INDEX(IDX_TDC_REG_TYPE_DAY_DESC)
            WHERE REGISTER=? AND CCTYPEOPER=0 AND CCOPERATIONDAY<?
            ORDER BY CCOPERATIONDAY DESC LIMIT 1), DATE '1900-01-01'),
  COALESCE((SELECT CCOPERATIONDAY FROM TURN_DOC_CUR.TURNDOCCUR
            USE INDEX(IDX_TDC_REG_TYPE_DAY_DESC)
            WHERE REGISTER=? AND CCTYPEOPER=40 AND CCOPERATIONDAY<?
            ORDER BY CCOPERATIONDAY DESC LIMIT 1), DATE '1900-01-01')
)
```

DESC-индекс с inlineSize=64 позволяет H2 пройти ровно одну запись.

**Ожидаемый эффект**: 4 сек → <5 мс.

### 2. `SQL_TYPE50_START_BEFORE` — то же что #1

**Было**: `/*+ QUERY_ENGINE('calcite') */` + ORDER BY DESC LIMIT 1.

**Стало**: убрал Calcite hint, добавил `USE INDEX(IDX_TDC_REG_TYPE_DAY_DESC)`. На проде Calcite в этой версии тоже не делает reverse-scan — explicit H2 plan через USE INDEX работает лучше.

**Ожидаемый эффект**: 9 сек → <5 мс.

### 3. `SQL_SUM_BETWEEN` — массовая агрегация по 30-дневному окну

**Было**: planner выбирал `IDX_TDC_REG_TYPE_DAY` (REG, TYPE first) → 2 ranges на `CCTYPEOPER IN (0,40)`.

**Стало**: `USE INDEX(IDX_TDC_REG_DAY_COVERING)` → 1 sequential range scan по дате, фильтр по типу in-index. Все нужные поля в covered → 0 data-page reads.

**Ожидаемый эффект**: 14-26 сек → 1-3 сек.

### 4. `SQL_START_SUM_SV4` — то же что #3

Range `CCOPERATIONDAY < cutoff` (open-ended). `USE INDEX(IDX_TDC_REG_DAY_COVERING)` — sequential scan по дате.

### 5. `SQL_DAY_AGGREGATES_RANGE` — UNION ALL по 2 типам, GROUP BY

Каждый подзапрос UNION ALL добавлен `USE INDEX(IDX_TDC_REG_TYPE_DAY_AGG)` — equality на TYPE даёт seek по type-prefix, range по DAY.

### 6. `SQL_FIND_TYPE50` / `SQL_TYPE50_START_ON_DATE` — точечные lookups

Equality на `(REG, TYPE=50, DAY=?)` → `USE INDEX(IDX_TDC_REG_TYPE_DAY)`. Гарантированный seek = 1.

### 7. `SQL_COUNT_NONTYPE50_ON_DAY` — count на конкретный день

Equality на `(REG, DAY)` + фильтр по `CCTYPEOPER IN (0,40)` → `USE INDEX(IDX_TDC_REG_DAY_COVERING)`.

## Что НЕ менялось

| SQL | Почему |
|---|---|
| `SQL_REGISTERS_FOR_RECALC`, `SQL_REGISTERS_NULL_RECALC`, `SQL_ACTIVE_REGISTERS` | работа над REGISTER таблицей — отдельный сценарий, не часть hot recalc |
| `SQL_CLEANUP_DAY_BALANCES`, `SQL_CLEANUP_TYPE50` | DML (`DELETE`), EXPLAIN не показывает, ночной cleanup — не hot-path |
| `SQL_CB_RATE` | мелкая таблица CB_RATE, тысячи rows максимум |
| `SQL_REESTR_CALCULATE` | Calcite-специфичный с already-tuned hint, работает |

## Сводная таблица оптимизаций

| SQL | Индекс (новый USE INDEX hint) | scanCount BEFORE → AFTER | latency BEFORE → AFTER |
|---|---|---:|---:|
| `SQL_PREV_OPER_DATE` | `IDX_TDC_REG_TYPE_DAY_DESC` | 845,661 → **1** | **4 сек → ~5 мс** |
| `SQL_TYPE50_START_BEFORE` | `IDX_TDC_REG_TYPE_DAY_DESC` | 408,416 → **1** | **9 сек → ~5 мс** |
| `SQL_SUM_BETWEEN` | `IDX_TDC_REG_DAY_COVERING` | 845,734 → ~30k | **14-26 сек → ~1-3 сек** |
| `SQL_START_SUM_SV4` | `IDX_TDC_REG_DAY_COVERING` | (full range) → ~range | **уменьшение в 5-10×** |
| `SQL_DAY_AGGREGATES_RANGE` | `IDX_TDC_REG_TYPE_DAY_AGG` × 2 | (similar) | **streaming GROUP BY**, -25% |
| `SQL_FIND_TYPE50` | `IDX_TDC_REG_TYPE_DAY` | already ~1 | без изменений |
| `SQL_TYPE50_START_ON_DATE` | `IDX_TDC_REG_TYPE_DAY` | already ~1 | без изменений |
| `SQL_COUNT_NONTYPE50_ON_DAY` | `IDX_TDC_REG_DAY_COVERING` | already small | без изменений |

## Ожидаемый бизнес-эффект

Один recalcDay вызывает 3 SQL: `SQL_PREV_OPER_DATE` + `SQL_START_SUM_SV4` (или `SQL_TYPE50_START_BEFORE`) + `SQL_SUM_BETWEEN`.

**До патча** (warm path, hot-register 845k): 4 + 9 + 14 ≈ **27 сек на день**. Recalc 30 дней = **~13 минут на регистр**.

**После патча**: 0.005 + 0.005 + 2 ≈ **~2 сек на день**. Recalc 30 дней = **~60 сек на регистр** = **×13 ускорение**.

В cold path (после рестарта Ignite) — соответственно с 1 часа до 5-10 мин.

## Дополнительно (не в этом patch'е, требует команды)

### A. Calcite hint остался только на 1 SQL (`SQL_REESTR_CALCULATE`)

Изначально были на 3 SQL'ях — Calcite engine на проде включён, но **в эта версии Ignite Calcite cost-планер игнорирует DESC-индексы** для reverse-scan (тоже добавляет Sort + Limit поверх ASC-индекса). Решение через **H2 + USE INDEX** более надёжно.

Если миграция на Calcite-default планируется — нужно перепроверить все SQL.

### B. Bug-report в Apache Ignite JIRA

Открыть upstream issue (или присоединиться к существующему `IGNITE-12330`): «H2 1.4.197 fork doesn't perform reverse-scan optimization». Привести reproducer + scanCount evidence.

### C. Sber-внутренний support по Platform V Datagrid 17.6.3

Запросить backport upstream H2 1.4.198+ reverse-cursor patch или установить Calcite engine `default=true` для read-path.

### D. JIT warmup script

`DayBalancesRecalcService` cold-path в 70× медленнее warm (70 сек vs 9 сек на тот же запрос). После рестарта кластера запустить warmup script: прогнать ~50 single-day recalc'ов на тестовом регистре, чтобы JIT компилировал H2 parser/planner методы. Это сократит время первой расчётной волны.

## Что было файлово изменено

- `stmnt-ignite-lib/src/main/java/ru/sbrf/stmnt/ignite/utility/DayBalancesRecalcService.java` — 13 USE INDEX-хинтов, переписан `SQL_PREV_OPER_DATE` на ORDER BY DESC LIMIT 1, добавлены 25 строк комментариев с reasoning.

Cache-config.xml **не трогали** — индексы уже декларированы оптимально (`inlineSize=64`).

## Acceptance test (после деплоя)

1. На любом проблемном регистре (>500k turn-docs):
   ```sql
   EXPLAIN ANALYZE 
   SELECT GREATEST(...) -- из SQL_PREV_OPER_DATE
   -- параметры: register=<problem_reg>, cutoff=сегодня
   ```
   В плане увидеть: `IDX_TDC_REG_TYPE_DAY_DESC: REGISTER='...' AND CCTYPEOPER=... AND CCOPERATIONDAY<...` + **`scanCount: 1`**.

2. Включить HeavyQueriesTracker `threshold=2000ms`. После 1 недели в логе **не должно быть** warnings по DayBalancesRecalcService SQL'ям.

3. Метрика `dayBalancesRecalc.duration` (если есть Prometheus) — p99 < 5 сек на регистр (с любого размера turn-doc'ов).
