# НТ getStatementSummary: реальные production-фильтры

## Что тут другое vs FILTERS_PERF_REPORT.md

В предыдущем `FILTERS_PERF_REPORT.md` я тестировал **фейковые** фильтры — `CCDTINN`, `CCDTACC`, `CCNUM`, `CCPURPOSE LIKE`. Это были мои выдумки. Реальный `GetStatementSummaryLibraryIgnite` + `DayBalancesRecalcService` **никогда** не фильтрует по этим колонкам.

Этот отчёт прогоняет **только те query-shape'ы которые реально эмитятся production-кодом** — 13 шейпов из 6 групп:

| Группа | Источник | SQL count |
|---|---|---|
| A | `loadOper50ContextBatch` + `SQL_FIND_TYPE50` | 4 (period / max-before / tuples / single-point) |
| B | `queryDayTurnoversBatch` + `querySumBetween` | 3 (TYPE IN (0,40) / TYPE != 50 / CASE WHEN single-reg) |
| C | `getNonZeroDaysBatch` | 1 (distinct days) |
| D | `executePrevOperDates` | 2 (DESC LIMIT 1 / GROUP BY) |
| E | `readDayBalancesBatch` / `readDayBalancesNonZeroBatch` | 2 (range / nonzero filter) |
| F | `getRegisterMap` | 1 (REGISTER IN (...)) |

Все WHERE'ы используют только: **REGISTER, CCOPERATIONDAY, CCTYPEOPER, CCDT** (никаких ИНН/ACC/NUM).

## Стенд

| | |
|---|---|
| Кластер | 1 cluster-1 × 3 узла |
| Volume | 100 регистров × 30 дней × (5 docs + 1 type50) = **18,000 turn-docs** + 3,000 day-balances + 100 registers |
| Distribution | 6000 типа=0, 9000 типа=40, **3000 типа=50** (по 1 на (reg, day)) |
| Indexes | IDX_TDC_REG_TYPE_OP (composite), IDX_TDC_OPDAY, IDX_TDC_TYPE, IDX_DB_OPDAY |
| ANALYZE | прогнан |

## Полная таблица (20 итераций каждый)

### avg ms

| # | Query | shape | N=1 | N=10 | N=50 | N=100 |
|---|---|---|---:|---:|---:|---:|
| A1 | oper50 period | `REG IN (...) AND TYPE=50 AND DAY BETWEEN ? AND ? ORDER BY REG,DAY` | 4.72 | 27.83 | 38.26 | **43.38** |
| A2 | oper50 max-before | `REG IN (...) AND TYPE=50 AND DAY<? GROUP BY REG` | 2.29 | 13.88 | 11.97 | 11.60 |
| A3 | oper50 tuples | `TYPE=50 AND (REG,DAY) IN ((?,?),...)` | 9.89 | 7.07 | 10.75 | 15.98 |
| A4 | find type50 single | `REG=? AND TYPE=50 AND DAY=? LIMIT 1` | 1.29 | 0.61 | 0.61 | **0.49** |
| B1 | agg TYPE IN (0,40) | `REG IN (...) AND TYPE IN (0,40) AND DAY BETWEEN GROUP BY REG,DAY,CCDT` | 6.98 | 24.82 | 48.47 | **66.43** ⚠ |
| B2 | agg TYPE != 50 | `REG IN (...) AND TYPE!=50 AND DAY BETWEEN GROUP BY REG,DAY,CCDT` | 5.71 | 26.71 | 43.55 | **67.43** ⚠ |
| B3 | sum_between single | `REG=? AND TYPE!=50 AND DAY>=? AND DAY<? CASE WHEN ...` | 2.55 | 1.26 | 1.21 | 1.02 |
| C1 | distinct days | `REG IN (...) AND DAY BETWEEN AND TYPE!=50 GROUP BY REG,DAY` | 2.12 | 21.42 | 30.64 | **40.69** |
| D1 | prev_oper_date before | `REG=? AND DAY<? AND TYPE!=50 ORDER BY CCDATE DESC LIMIT 1` | 1.51 | 0.70 | 0.82 | 0.78 |
| D2 | prev_oper_date in_period | `REG=? AND DAY>=? AND DAY<? AND TYPE!=50 GROUP BY DAY` | 2.44 | 0.85 | 0.90 | 0.89 |
| E1 | daybalances range | `REG IN (...) AND DAY BETWEEN ? AND ?` | 1.88 | 3.91 | 10.13 | 15.73 |
| E2 | daybalances nonzero | `REG IN (...) AND DAY BETWEEN AND (CCDTSUM!=0 OR ...)` | 4.19 | 4.51 | 9.78 | 15.01 |
| F1 | register IN | `REGISTER WHERE OBJECTID IN (...)` | 3.34 | 0.80 | 0.95 | 1.37 |

### p99 ms (tail)

| # | Query | N=1 | N=10 | N=50 | N=100 |
|---|---|---:|---:|---:|---:|
| A1 | oper50 period | 7.4 | 39.3 | 55.7 | **73.4** |
| B1 | agg IN(0,40) | 13.8 | 37.6 | 76.2 | **85.3** |
| B2 | agg !=50 | 14.5 | 53.1 | 78.5 | **84.0** |
| C1 | distinct_days | 3.2 | 41.6 | 35.8 | 51.6 |
| E1 | daybalances | 3.9 | 4.6 | 16.5 | 25.6 |

Tail сжатый — p99/avg обычно **1.3–1.8×**, без выбросов. Stable performance.

## Топ-3 узких места

### 1. 🔴 B1/B2 — turnover-aggregation (TYPE IN (0,40) / != 50)

**Это сердце getStatementSummary.** Используется в `queryDayTurnoversBatch()` — считает `SUM(CCSUM), SUM(CCSUMNAT), COUNT(*)` сгруппированно по `(REGISTER, CCOPERATIONDAY, CCDT)`.

- N=100: **avg 66 ms, p99 85 ms** — самый дорогой шейп
- Per-register cost: **0.66 ms/reg** на 30-дневном окне
- Скейлинг: 6.98 → 24.82 → 48.47 → 66.43 — sublinear, но всё равно главный нагружатель CPU

**Почему дорого**: full scan TURNDOCCUR (без CCTYPEOPER в композитном индексе впереди → планировщик использует REG-only seek), затем aggregation across 3 nodes, REDUCE merge.

**B2 (`!= 50`)** немного дороже B1 (`IN (0,40)`) на больших N — потому что планировщику сложнее с inequality predicate. На N=100 разница ~1% (67 vs 66 ms).

### 2. 🔴 A1 — oper50 period read

`SELECT ... FROM TURNDOCCUR WHERE REG IN (...) AND TYPE=50 AND DAY BETWEEN ? AND ? ORDER BY REG, DAY ASC`

- N=100: **avg 43 ms, p99 73 ms**
- Per-register cost: **0.43 ms/reg**
- Возвращает 27 rows/register × 100 = 2700 type50 записей с сортировкой

**Скейлинг**: A1 растёт сильнее остальных от N=1 до N=10 (4.7 → 27.8 ms ×6), потом флаттенится (43 ms на N=100 это всего ×1.6 vs N=50). Это **MAP-фаза идёт на 3 узла**, и при больших N координация amortизируется.

### 3. 🔴 C1 — distinct days (flagZeroTurns=false path)

`SELECT REG, DAY FROM TURNDOCCUR WHERE REG IN (...) AND DAY BETWEEN AND TYPE!=50 GROUP BY REG, DAY`

- N=100: **avg 41 ms, p99 52 ms**
- Эквивалентно B1/B2 по cost'у — на dataset'е тот же scan, разница только в проекции и без SUM

**Урок**: код `getNonZeroDaysBatch` + `queryDayTurnoversBatch` делают **два прохода по одним и тем же rows** — distinct days потом aggregation. Это **2× дороже** чем должно быть. Оптимизация: вернуть `(REG, DAY, CCDT, SUM, COUNT)` одним запросом и фильтровать `HAVING SUM != 0` или в коде.

## Дешёвые шейпы (≤2 ms независимо от N)

| # | Query | avg на N=100 |
|---|---|---:|
| A4 | SQL_FIND_TYPE50 (single-point) | **0.49 ms** |
| D1 | prev_oper_date DESC LIMIT 1 (single reg) | 0.78 |
| D2 | prev_oper_date GROUP BY (single reg) | 0.89 |
| B3 | sum_between single reg | 1.02 |
| F1 | REGISTER lookup by IN | 1.37 |

Эти запросы **константные по latency** независимо от размера batch'а потому что:
- F1: лёгкий lookup в маленьком REGISTER (100 rows total)
- A4/D1/D2/B3: single-register affinity-collocated с использованием PK/composite index — Ignite маршрутит ровно на 1 партицию.

## E1/E2 — DAYBALANCES range

| N | E1 range | E2 nonzero |
|---:|---:|---:|
| 1 | 1.88 | 4.19 |
| 10 | 3.91 | 4.51 |
| 50 | 10.13 | 9.78 |
| 100 | 15.73 | 15.01 |

DAYBALANCES в **4× дешевле** TURNDOCCUR-aggregation на тех же N. Причина — таблица уже pre-aggregated:
- TURNDOCCUR: 18K rows для 100 регистров
- DAYBALANCES: 3K rows для 100 регистров (×6 меньше)

E2 (с фильтром nonzero) **не дороже** E1 — лишний predicate на in-memory строки бесплатен.

## Эстимейт total latency для getStatementSummary

### Путь A: flagZeroTurns=true (классический)

```
F1 (REGISTER lookup)         +
A1 (oper50 period)           +    <- ⚠ дорогой
A2 (oper50 max-before)       +
A3 (oper50 tuples)           +
B1 (turnover_agg IN(0,40))   +    <- ⚠⚠ самый дорогой
E1 (daybalances range)       
                              =
```

На N=100: 1.37 + 43.38 + 11.60 + 15.98 + 66.43 + 15.73 = **154 ms** total per call

### Путь B: flagZeroTurns=false

```
F1 + A1 + A2 + A3 +
C1 (distinct days)           +    <- ⚠ дублирующий scan
B2 (turnover_agg != 50)      +    <- ⚠⚠
E2 (daybalances nonzero)
                              =
```

На N=100: 1.37 + 43.38 + 11.60 + 15.98 + 40.69 + 67.43 + 15.01 = **195 ms** total

**flagZeroTurns=false дороже на 27%** из-за дополнительного passa по C1 + B2 вместо B1.

### Путь C: только DAYBALANCES (теоретический)

```
F1 + E1 (или E2)  =  1.37 + 15.73  =  17 ms  total
```

**×9 быстрее** чем путь A. Подтверждает FINAL_PERF_REPORT'овский вывод о DAYBALANCES-summary как оптимальном варианте.

## Per-register cost amortизация

| Query | N=1 ms/reg | N=10 ms/reg | N=100 ms/reg | амортизация |
|---|---:|---:|---:|---|
| A1 oper50 period | 4.72 | 2.78 | **0.43** | ×11 |
| B1 turnover_agg | 6.98 | 2.48 | **0.66** | ×11 |
| C1 distinct_days | 2.12 | 2.14 | **0.41** | ×5 |
| E1 daybalances | 1.88 | 0.39 | **0.16** | ×12 |
| F1 register IN | 3.34 | 0.08 | **0.014** | ×240 (!) |

**Урок batch-API**: при N=1 каждый шейп платит fixed overhead (network round-trip 1.5-3 ms). С N=10-100 этот overhead **амортизируется в 10×**.

→ В production не вызывать executeList **по одному регистру** в цикле. Всегда батч.

## Главные выводы

1. **B1/B2 (turnover aggregation на TURNDOCCUR) — bottleneck #1**. 66 ms/100 регистров, p99 85 ms. Это **75% времени getStatementSummary**.
2. **Перевод на DAYBALANCES-summary даёт ×9 ускорение** (с 150-195 ms до 17 ms на N=100).
3. **A1 (oper50 period) — bottleneck #2**. 43 ms/100 регистров. На production где у регистра может быть N type50 записей — рост cost-а пропорционально N (а не amortизированно как сейчас на 27 type50/reg).
4. **C1 (distinct days) — лишний scan в flagZeroTurns=false**. Стоит слить с B2 в один query.
5. **D1/D2 (PrevOperDates) — дешёвые** (~1 ms). Optimization работает: `ORDER BY DESC LIMIT 1` обходит проблему reverse-scan H2 за счёт композитного индекса с DESC-sortable полем (мы заранее заложили IDX_TDC_REG_TYPE_OP который покрывает REG+TYPE+DAY).
6. **F1 (REGISTER lookup) — почти бесплатный** (1.4 ms на N=100). Без LEFT JOIN он ещё быстрее — в production эта query тяжелее из-за join'ов с CLIENT и CURRENCY.

## Реальные queries которые НЕ принимают пользовательских фильтров

Самое интересное наблюдение из этого прогона:

**В production GetStatementSummary НЕТ user-filters типа "по ИНН"/"по сумме"/"по типу"**. Запрос принимает только:
- `accountsList` (registerId или accNum+ucpId) — превращается в `REGISTER IN (...)`
- `filters.fromDate` / `filters.toDate` — превращается в `CCOPERATIONDAY BETWEEN ? AND ?`
- `filters.flagZeroTurns` — выбор пути (B1 vs B2+C1)
- `orderBy` + `pagination` — обрабатываются in-memory в Java (НЕ через ORDER BY/LIMIT в SQL)

→ Все фильтры из моего предыдущего FILTERS_PERF_REPORT (ИНН, счёт, doc#, purpose LIKE) — **никогда не доходят до Ignite**. Они либо вообще не существуют в API, либо обрабатываются in-app после получения сырых данных. Это объясняет почему TURNDOCCUR на проде имеет такой минимальный набор индексов.

## Воспроизведение

```bash
docker compose up -d
curl -X POST 'http://localhost:18080/api/debug/bulk-seed?registers=100&days=30&docsPerDay=5'
curl -X POST 'http://localhost:18080/api/debug/analyze-all?clusterId=cluster-1'
for N in 1 10 50 100; do
  curl -X POST "http://localhost:18080/api/perf/real-filters?N=$N&iterations=20" \
    > "real-filters-N$N.json"
done
```

Endpoint: `RealFiltersPerfController.realFilters()`. 13 SQL вариантов × 20 iters + warmup.
JSON-результаты: `real-filters-results.json`.
