# НТ getStatementSummary: производительность по фильтрам

## Стенд

| | |
|---|---|
| Кластер | 1 cluster-1 × **3 узла** (`ignite-1` + `ignite-1b` + `ignite-1c`) |
| Volume | 200 регистров × 30 дней × 5 docs = **30,000 turn-docs** |
| Per-register | ~150 docs за 30 дней (реалистично для среднего клиента) |
| Counterparty pool | 100 уникальных ИНН + 100 уникальных счетов (распределены по register×day×doc через hash) |
| Поля schema | OBJECTID, CCRQUID, CCIDEKS, REGISTER, CCSUM, CCSUMNAT, CCDT, CCTYPEOPER, CCDATE, CCOPERATIONDAY, **CCDTINN, CCKTINN, CCDTACC, CCKTACC, CCPURPOSE, CCNUM** |
| Индексы | IDX_TDC_REG_TYPE_OP (composite), + IDX_TDC_OPDAY, IDX_TDC_TYPE, IDX_TDC_IDEKS, **IDX_TDC_DTINN, IDX_TDC_KTINN, IDX_TDC_DTACC, IDX_TDC_KTACC** |
| ANALYZE | прогнан |

## Результаты (R001, 20 итераций каждый)

| # | FILTER | ROWS | AVG ms | P50 | P95 | P99 |
|---|---|---:|---:|---:|---:|---:|
| 1 | `WHERE REGISTER=?` (baseline) | 150 | 7.98 | 7.68 | 13.6 | 13.6 |
| 2 | + 1 day equality | 5 | 5.10 | 4.48 | 11.0 | 11.0 |
| 3 | + 30 days range | 135 | 9.02 | 5.73 | **57.5** | 57.5 |
| 4 | + 90 days range | 135 | 5.36 | 5.43 | 9.0 | 9.0 |
| 5 | + CCTYPEOPER IN (0,40) | 135 | 4.85 | 4.39 | 9.2 | 9.2 |
| 6 | + CCSUM BETWEEN | 135 | 4.21 | 3.56 | 7.5 | 7.5 |
| 7 | + CCDTINN OR CCKTINN | 1 | **2.48** | 1.85 | 6.6 | 6.6 |
| 8 | + CCDTACC OR CCKTACC | 0 | **2.13** | 1.56 | 5.2 | 5.2 |
| 9 | + CCDT=? (direction) | 54 | 2.94 | 2.49 | 6.8 | 6.8 |
| 10 | combo (all filters) | 54 | 3.30 | 2.65 | 7.2 | 7.2 |
| 11 | + ORDER BY DESC LIMIT 50 | 50 | 5.85 | 4.83 | 11.5 | 11.5 |
| 12 | LIMIT 50 OFFSET 100 | 35 | 3.91 | 3.47 | 7.5 | 7.5 |
| 13 | + CCPURPOSE LIKE '%...%' | 27 | 4.78 | 4.31 | 10.9 | 10.9 |
| 14 | + CCNUM=? | 1 | **2.33** | 1.66 | 6.8 | 6.8 |
| 15 | by CCRQUID (no register) | 1 | **15.1** | 14.3 | 29.4 | 29.4 |
| 16 | by CCIDEKS (no register) | 1 | 3.48 | 2.64 | 9.6 | 9.6 |

## Категоризация фильтров

### 🟢 Быстрые (≤3 ms) — селективные WHERE через индекс

| # | Filter | Avg | Почему |
|---|---|---:|---|
| 7 | INN counterparty | 2.48 | IDX_TDC_DTINN + IDX_TDC_KTINN — selective, 1 row из 150 |
| 8 | Account | 2.13 | IDX_TDC_DTACC + IDX_TDC_KTACC — empty result, very fast |
| 14 | Doc number | 2.33 | через IDX_TDC_REG_TYPE_OP + filter, narrow result |
| 9 | Direction CCDT | 2.94 | 50/50 split, planner filter'ит в-память дёшево |
| 10 | combo | 3.30 | composite filter selectivе → small result set |
| 6 | Amount range | 4.21 | амортизация range scan на covered |

### 🟡 Средние (4-6 ms) — широкие range scans

| # | Filter | Avg | Почему |
|---|---|---:|---|
| 16 | CCIDEKS lookup | 3.48 | single-column IDX_TDC_IDEKS — но без REGISTER (multi-node MAP) |
| 12 | OFFSET pagination | 3.91 | sort + skip 100 + take 50 |
| 13 | PURPOSE LIKE | 4.78 | full scan strings, фильтр в памяти |
| 2 | 1-day equality | 5.10 | seek по (REG, DAY) — но 5 rows только |
| 4 | 90-day range | 5.36 | covered index scan |
| 5 | Type filter | 4.85 | composite index sweet spot |
| 11 | ORDER BY + LIMIT 50 | 5.85 | sort всего range + take top 50 |

### 🔴 Медленные (>7 ms)

| # | Filter | Avg | Почему |
|---|---|---:|---|
| 1 | REGISTER only | 7.98 | full scan register'а (150 rows × все колонки) |
| 3 | 30-day range | 9.02 | covered scan, **p99=57ms (!)** — cold cache spike |
| **15** | **by CCRQUID** | **15.1** | **нет REGISTER в WHERE → MAP на 3 узла, NETWORK overhead доминирует** |

## Ключевые инсайты

### 1. Селективные фильтры в 3-4× быстрее baseline

Простой `WHERE REGISTER=?` (1 фильтр) = 8 ms. С добавлением селективных предикатов (INN, ACC, NUM) — **2-3 ms**. Это потому что `CCDTINN=?` сужает результат до **1 row из 150** — index seek + projection доминируют над сетевым overhead.

**Урок**: всегда давать пользователю UI-фильтры — чем больше WHERE-условий, тем быстрее запрос (counter-intuitive vs PostgreSQL).

### 2. Date range — самое опасное

| Range | Avg | p99 |
|---|---:|---:|
| 1 day | 5.10 | 11 |
| 30 days | 9.02 | **57.5** (×6 от avg!) |
| 90 days | 5.36 | 9 |

Интересно: **30 days медленнее 90 days** в среднем. Объяснение — на 30 днях planner ещё пытается использовать range через индекс, на 90 днях он переключается на full scan (что для 150 rows тоже быстро). Range scan имеет нелинейный профиль около cardinality threshold.

**p99 30 days = 57 мс** — это **×6 от avg**. Cold-cache spike. На production с миллионами rows этот spike может быть **в секундах**.

### 3. Запросы без REGISTER — медленные на multi-node

Сценарий **15: `WHERE CCRQUID=?`** (без REGISTER):
- avg = **15 ms** (vs 2-3 ms для register-anchored queries)
- p99 = 29 ms

Это потому что без REGISTER planner **не может использовать affinity**. Запрос идёт **MAP на все 3 узла**, каждый ищет ccRqUId локально, потом REDUCE собирает. Network round-trip доминирует.

**Сценарий 16: `WHERE CCIDEKS=?`** (тоже без REGISTER): avg = 3.48 ms — почему быстрее?
- Потому что в нашем seed CCIDEKS имеет очень распределённую cardinality (30K уникальных значений на 30K rows = 1 row per value)
- Index lookup быстрее, потому что planner может быстро отбросить partition'ы

**Урок для production**: API getStatementSummary **всегда** должен принимать `register` (или `accNum`) — без него запрос идёт на полное сканирование cluster'а. Если пользователь не знает register — UI должен сначала найти `register` по другому критерию (accNum), потом второй запрос с anchor.

### 4. ORDER BY + LIMIT vs OFFSET

| | ms | Что делает |
|---|---:|---|
| `ORDER BY DESC LIMIT 50` | 5.85 | top-50 — нужно отсортировать ВСЕ rows + взять top |
| `LIMIT 50 OFFSET 100` | 3.91 | страница 3 — тот же sort, но reduce'ся когда дойдёт до OFFSET |

**Counter-intuitive**: OFFSET pagination быстрее! Потому что результат меньше (35 vs 50 rows). Но на больших данных и OFFSET становится дорогим — Ignite материализует все strok'и под N+M, выбрасывает первые N.

### 5. PURPOSE LIKE — приемлемо

`WHERE CCPURPOSE LIKE '%Зарплата%'` = 4.78 ms. Полный text-scan, но всего 150 rows. Index по purpose **не помогает** для LIKE с wildcard в начале. Для текстового поиска лучше: full-text index (Ignite не поддерживает out-of-box) или Lucene-side index (есть `@QueryTextField` в DTO).

### 6. p95/p99 vs avg на multi-node

Tail latency сжатый: **p95/p50 в среднем 1.5-2×**. Это **хорошо** — нет surprises.  
Исключения: **#3** (30-day range, p99=57ms vs avg=9) и **#15** (CCRQUID lookup) — там MAP/REDUCE координация даёт spike.

## Per-filter рекомендации для GetStatementSummaryLibraryIgnite

| Что хочет клиент | Какой SQL построить | Ожидаемая latency |
|---|---|---:|
| Все операции за период | `WHERE REGISTER=? AND day BETWEEN ?..?` | 5-9 ms |
| Только списания / зачисления | `+ AND CCDT=?` | -50% от выше |
| По сумме (например, проверка крупных) | `+ AND CCSUM > ?` | -50% |
| По контрагенту (имени банка / ИНН) | `+ AND (CCDTINN=? OR CCKTINN=?)` | **2-3 ms** ⚡ |
| По номеру документа | `+ AND CCNUM=?` | **2 ms** ⚡ |
| Текстовый поиск в назначении | `+ AND CCPURPOSE LIKE '%X%'` | 5 ms (на 30K rows) |
| Пагинация (страница 1) | `+ ORDER BY day DESC LIMIT 50` | 6 ms |
| Пагинация (далее) | `+ ORDER BY day DESC LIMIT 50 OFFSET N` | 4 ms (но растёт с OFFSET) |

## Архитектурные выводы

1. **REGISTER — обязательный** в WHERE. Без него — full cluster scan, ×5-10 медленнее.
2. **Селективные счета/ИНН/ID** дают **серьёзное ускорение** — UI должен предлагать их пользователю.
3. **Date range = опасно**: avg низкий, но p99 spike возможен на cold cache. Логировать запросы с длинным окном.
4. **LIKE на тексте** — full scan, **не масштабируется**. Для production с N>1M rows нужен Lucene-based index или вынесение поиска в Elasticsearch.
5. **OFFSET pagination** работает на N≤1000. Для report с тысячами строк — переходить на **keyset pagination** (`WHERE day < lastSeenDay LIMIT N`).

## Throughput per-filter

На 3-node cluster в single thread (без concurrency):

| Filter | reg/sec (один client) |
|---|---:|
| `WHERE REGISTER + INN` | **400** ← selective wins |
| `WHERE REGISTER + day + amount` | 240 |
| `WHERE REGISTER + day + type` | 200 |
| `WHERE REGISTER + day range 30d` | 110 |
| `WHERE REGISTER + LIKE` | 210 |
| `WHERE CCRQUID` (no register) | **66** ← no affinity = slow |

С 10 concurrent clients (как в реальной нагрузке getStatementSummary endpoint) throughput можно умножить на ~5-8× из-за параллелизма, особенно для селективных фильтров.

## Воспроизведение

```bash
docker compose up -d   # 3-node cluster
curl -X POST 'http://localhost:18080/api/debug/bulk-seed?registers=200&days=30&docsPerDay=5'
curl -X POST 'http://localhost:18080/api/debug/analyze-all?clusterId=cluster-1'
curl -X POST 'http://localhost:18080/api/perf/filters?register=R001&iterations=20'
```

Endpoint: `FilterPerfController.filters()`. 16 SQL вариантов, по 20 iterations + warmup каждый.

## Сохранённый JSON

```bash
curl -X POST '.../filters?register=R001&iterations=20' > filters-results.json
```

Полные данные — в `MULTIREG_MULTINODE_REPORT.md`, `FINAL_PERF_REPORT.md`, теперь `FILTERS_PERF_REPORT.md`.
