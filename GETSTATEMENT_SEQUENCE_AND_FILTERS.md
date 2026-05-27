# GetStatementSummary: полный sequence + все фильтры

Источник истины: `GetStatementSummaryLibraryIgnite.java` (1308 строк), методы `execute()` и `executeList()`.

## 1. Все параметры request DTO

`GetStatementSummary` имеет **5 групп параметров**, из них **3 эмитят SQL**, **2 — обрабатываются в памяти Java**.

### 1.1. Влияют на SQL

| Поле | Тип | Куда попадает в WHERE | Обязательность |
|---|---|---|---|
| **`accountsList[].registerId`** | List\<String\> | `r.CCREGISTERID IN (?,?,...)` | один из ID required |
| **`accountsList[].accNum`** + **`ucpId`** | pairs | `(r.CCACCNUM, r.CLIENT) IN ((?,?),...)` | альтернатива registerId |
| **`filters.fromDate`** | Date | `CCOPERATIONDAY >= ?` (всюду) | optional |
| **`filters.toDate`** | Date | `CCOPERATIONDAY <= ?` (всюду) | optional |
| **`filters.flagZeroTurns`** | Boolean | **переключает PATH** SQL (true → B1+E1; false → C1+B2+E2) | optional, default=true |

### 1.2. Обрабатываются в памяти Java (НЕ влияют на SQL)

| Поле | Тип | Что делает | Где |
|---|---|---|---|
| `orderBy.orderByColumn` | "accNum" / "dateBalances" / "clientName" / "clientINN" | строит `Comparator<DateAccountBalances>` | `ConcurrentSkipListSet` |
| `orderBy.orderDesc` | Boolean | направление 1-го уровня | Comparator.reverseOrder() |
| `orderBy.orderDescL2` | Boolean | направление tiebreaker | thenComparing(...) |
| `pagination.pageSize` | Integer | `.limit(pageSize)` | Stream API на NavigableSet |
| `pagination.offset` | BigDecimal | `.skip(offset)` | Stream API на NavigableSet |

**Это ключевое открытие**: orderBy и pagination обрабатываются **post-SQL** на full result-set в JVM, а не как `ORDER BY ... LIMIT N OFFSET M` в Ignite. Все строки сначала тянутся, потом сортируются и режутся в памяти.

### 1.3. Meta (не фильтры)

`version`, `rqTm`, `rqUID`, `requestSystemId` — только для валидации/трассировки.

### 1.4. Параметр уровня сервиса (не из request)

| Поле | Источник | Эффект |
|---|---|---|
| **`noPeriod`** | аргумент `executeList(request, noPeriod)` | если `true` — расширяет maxDate/minDate до min(register.openDate) / today |
| `dayBalancesProperties.enabled` | application.yml | вкл/выкл фазу `db` (читать предсосчитанные DAYBALANCES) |
| `dayBalancesProperties.periodEndDays` | application.yml | граница `db` vs `postDb` фаз |

---

## 2. Sequence diagram — что происходит при `executeList()`

```
HTTP / RMI
   │
   ▼
GetStatementSummaryServiceImpl
   │
   ▼
GetStatementSummaryLibraryIgnite.executeList(request, noPeriod)
   │
   ├─ validateParams() — throw if пустые obligatory
   │
   ├─ ───────────  STEP 1: Резолв регистров ──────────────
   │       SQL: F1 (если есть registerId)
   │            SELECT r.* FROM REGISTER r
   │              LEFT JOIN CLIENT c ON r.CLIENT=c.OBJECTID
   │              LEFT JOIN CURRENCY cur ON r.CURRENCY=cur.OBJECTID
   │              WHERE r.CCREGISTERID IN (?,?,...)
   │
   │       SQL: F2 (если есть accNum+ucpId)
   │            WHERE (r.CCACCNUM, r.CLIENT) IN ((?,?),...)
   │
   │       → registerMap (Set<Register>)
   │
   ├─ ─────────── STEP 2: Расчёт period ──────────────────
   │       minDate = max(filters.fromDate, register.minDate, simpleValidator.periodLimitDate)
   │       maxDate = min(filters.toDate, register.maxDate, today)
   │       daysCount = maxDate - minDate + 1
   │       → DayBalancesPeriod(preDb, db, postDb) — 3 фазы разбивки окна
   │
   ├─ ─────────── STEP 3 (a): flagZeroTurns=true ──────────
   │       (для каждой phase: preDb, db, postDb)
   │
   │       SQL: A1  loadOper50ContextBatch() — type50 в окне периода
   │            SELECT REGISTER, CCOPERATIONDAY, CCSTARTSUM, CCSTARTSUMNAT
   │              FROM TURNDOCCUR
   │             WHERE REGISTER IN (...) AND CCTYPEOPER=50
   │               AND CCOPERATIONDAY BETWEEN ? AND ?
   │             ORDER BY REGISTER, CCOPERATIONDAY ASC
   │
   │       SQL: A2  MAX(date) для type50 ДО fromDate
   │            SELECT REGISTER, MAX(CCOPERATIONDAY)
   │              FROM TURNDOCCUR
   │             WHERE REGISTER IN (...) AND CCTYPEOPER=50 AND CCOPERATIONDAY < ?
   │             GROUP BY REGISTER
   │
   │       SQL: A3  Точечные чтения по (REG, MAX_DAY) tuples
   │            SELECT REGISTER, CCOPERATIONDAY, CCSTARTSUM, CCSTARTSUMNAT
   │              FROM TURNDOCCUR WHERE CCTYPEOPER=50
   │               AND (REGISTER, CCOPERATIONDAY) IN ((?,?),...)
   │
   │       SQL: B1  queryDayTurnoversBatch() — агрегация оборотов
   │            SELECT REGISTER, CCOPERATIONDAY, CCDT,
   │                   SUM(COALESCE(CCSUM,0)), SUM(COALESCE(CCSUMNAT,0)), COUNT(*)
   │              FROM TURNDOCCUR
   │             WHERE REGISTER IN (...) AND CCTYPEOPER IN (0,40)
   │               AND CCOPERATIONDAY BETWEEN ? AND ?
   │             GROUP BY REGISTER, CCOPERATIONDAY, CCDT
   │
   │       SQL: E1  readDayBalancesBatch() — pre-aggregated баланс
   │            SELECT REGISTER, CCBALANCEDATE, ...
   │              FROM DAYBALANCES
   │             WHERE REGISTER IN (...) AND CCBALANCEDATE BETWEEN ? AND ?
   │
   │       → calcWithZeroTurnsOptimized() — restoring sum через oper50 + turnovers
   │
   ├─ ─────────── STEP 3 (b): flagZeroTurns=false ─────────
   │       Дополнительные SQL (вместо B1+E1):
   │
   │       SQL: C1  getNonZeroDaysBatch() — distinct (reg, day) с ненулевыми оборотами
   │            SELECT REGISTER, CCOPERATIONDAY FROM TURNDOCCUR
   │             WHERE REGISTER IN (...) AND CCOPERATIONDAY BETWEEN ? AND ?
   │               AND CCTYPEOPER != 50
   │             GROUP BY REGISTER, CCOPERATIONDAY
   │
   │       SQL: B2  queryDayTurnoversBatch(op1Only=true) — тоже самое что B1
   │              но с предикатом TYPE != 50
   │
   │       SQL: E2  readDayBalancesNonZeroBatch()
   │            ... AND (CCDTSUM != 0 OR CCKTSUM != 0 OR CCDTSUMNAT != 0 OR CCKTSUMNAT != 0)
   │
   ├─ ─────────── STEP 4: Empty DAYBALANCES fallback ─────
   │       Для регистров без записей в DAYBALANCES в `db`-фазе:
   │         a. если есть preDb → берёт outcome последнего дня preDb
   │         b. иначе postDb → берёт income первого дня postDb
   │         c. иначе → getBalancesForDateBatch() → еще один A1+A2+A3 вызов
   │
   ├─ ─────────── STEP 5: In-memory orderBy/pagination ───
   │       (нет SQL — всё в JVM)
   │
   │       Comparator<DateAccountBalances> по orderBy.column:
   │         - "dateBalances" → date DESC/ASC, accNum, registerId (default)
   │         - "accNum"       → ccAccNum, date, registerId
   │         - "clientName"   → CCNAME, date, accNum, registerId
   │         - "clientINN"    → CCINN, date, accNum, registerId
   │
   │       accBalancesSet = ConcurrentSkipListSet<>(comparator)
   │       result = accBalancesSet.stream()
   │                              .skip(pagination.offset)
   │                              .limit(pagination.pageSize)
   │                              .collect(...)
   │
   ├─ ─────────── STEP 6: CB rates conversion ───────────
   │       Для каждого AccBalances с валютой != "810"/"643":
   │         SQL: G1  (cbRatesService.get(currencyCode, date))
   │            SELECT CCRATE FROM CB_RATE WHERE CCCODE=? AND CCDATE=?
   │       Конвертит sumNat в рубли по курсу ЦБ.
   │
   └─ return SendStatementSummary
```

### Total SQL per call по путям

| flagZeroTurns | accBy regId | Уникальных SQL-shape'ов | Раз с RegisterId=N |
|---|---|---|---|
| true | yes | F1 + A1 + A2 + A3 + B1 + E1 (+ G\*) | по 1 запросу × фазы (preDb/db/postDb) |
| true | no (accNum) | F2 + A1 + A2 + A3 + B1 + E1 | то же |
| false | yes | F1 + A1 + A2 + A3 + **C1 + B2 + E2** | то же |
| false | no (accNum) | F2 + A1 + A2 + A3 + C1 + B2 + E2 | то же |

При `noPeriod=true`: окно расширяется до `register.openDate..today` — те же шейпы, но больше rows × дольше.

---

## 3. Полная таблица filter dimensions и их эффект

| # | Dimension | Значения | Эффект на SQL | Эффект на latency |
|---|---|---|---|---|
| 1 | `accountsList.*` | by `registerId` / by `accNum+ucpId` / **mixed** | F1 vs F2 vs F1+F2 | F1 быстрее (1 IN-clause); F2 медленнее (tuple IN на 2 поля); mixed = 2 запроса |
| 2 | N регистров в `accountsList` | 1 / 10 / 50 / 100 / 500 / 1000 | placeholders в IN-clause; > 50 — H2 может переключиться на full scan | per-reg amortизируется ×10–×240 |
| 3 | `filters.fromDate` + `toDate` | присутствуют / отсутствуют | `BETWEEN ? AND ?` появляется/исчезает | окно × per-day cost |
| 4 | period length | 1d / 30d / 90d / **noPeriod** | range size в BETWEEN | 1d → seek; 30d → range scan; noPeriod → ВСЯ история |
| 5 | `filters.flagZeroTurns` | true (default) / false | true → B1+E1; false → C1+B2+E2 | false на 27% дороже (доп. C1 scan) |
| 6 | `orderBy.column` | dateBalances (default) / accNum / clientName / clientINN | НИ ОДНОГО SQL не меняется | только in-memory Comparator |
| 7 | `orderBy.orderDesc` / `orderDescL2` | true/false | НИ ОДНОГО SQL не меняется | только in-memory |
| 8 | `pagination.pageSize` | 50 (default) / 100 / 500 / 1000 | НИ ОДНОГО SQL не меняется | определяет сколько результатов после .limit() в JVM |
| 9 | `pagination.offset` | 0 / N | НИ ОДНОГО SQL не меняется | .skip(offset) в JVM **тянет все rows** |
| 10 | `noPeriod` | true / false | без `BETWEEN` если true | минимальная latency для коротких окон |
| 11 | `dayBalancesProperties.enabled` | true / false | вкл/выкл E1/E2 (db-фазу) | false → всё считается через TURNDOCCUR (×2–9 медленнее) |

---

## 4. Ключевая ловушка: pagination + orderBy не пробрасываются в SQL

```java
NavigableSet<DateAccountBalances> accBalancesSet
    = new ConcurrentSkipListSet<>(dayBalanceComparator);

// PASS 1: собрать ВСЕ rows за период
// (SQL A1 + B1 + E1 — ВСЕ строки)
accBalancesSet.addAll(allTheRows);

// PASS 2 (только при flagZeroTurns=false): post-skip/limit в JVM
accBalancesSet.stream()
    .skip(offset)
    .limit(pageSize)
    .forEach(...)
```

**Следствия:**
- `pagination.pageSize=50` на периоде 90 дней × 1000 регистров = **тянем 90,000 rows**, чтобы отдать 50. Запрос в Ignite **не получает LIMIT 50** — он отдаёт всё.
- `pagination.offset=10000` — те же 90,000 rows тянутся, потом skip'ятся.
- `orderBy.column="clientName"` сортирует **в памяти JVM** после получения всех rows. На больших N — это ещё и память.

**Рекомендация production-команде**: если pagination на больших N — переписать на **keyset pagination в SQL** (`WHERE day < lastSeen LIMIT pageSize`). Сейчас pagination — ложная.

---

## 5. Сводка perf по всем filter-комбинациям

Все цифры — из `REAL_FILTERS_PERF_REPORT.md` (3-node cluster, 100 regs × 30 дней × 6 docs = 18K turn-docs).

### 5.1. По filter dimensions

| Filter | Влияние на total latency | Замер |
|---|---|---|
| **N=1 → N=100 регистров** | ×9 для total, но ×11–240 amortизация per-register | F1: 3.34 → 0.014 ms/reg; B1: 6.98 → 0.66 ms/reg |
| **30d → 90d period** | ×1.5–2 для tail; на больших N выравнивается | A1 + B1 на 90d ~ ×1.8 от 30d |
| **flagZeroTurns true → false** | +27% latency (доп. C1 scan + B2 чуть дороже B1) | 154 ms → 195 ms на N=100 |
| **registerId → accNum+ucpId** | F1 ~1ms → F2 ~3ms (tuple IN дороже) | замер не сделан напрямую; F2 структурно идентичен A3 (tuple IN) который 16ms на N=100 |
| **dayBalances on → off** | ×9 (155 → ~17 ms vs full TURNDOCCUR-only path) | См. путь C в REAL_FILTERS report |
| **pagination.pageSize 50 vs 1000** | 0% от SQL; только JVM stream `.limit()` | трать ms на in-memory sort |
| **orderBy column** | 0% от SQL; только JVM Comparator | trivial |

### 5.2. Per-path total (N=100, 30 дней)

| Путь | Шейпы SQL | Total |
|---|---|---:|
| flagZeroTurns=true, db enabled | F1 + A1 + A2 + A3 + B1 + E1 | **154 ms** |
| flagZeroTurns=false, db enabled | F1 + A1 + A2 + A3 + C1 + B2 + E2 | **195 ms** |
| DayBalances-only (теоретический) | F1 + E1 | **17 ms** ⚡ |

### 5.3. Tail latency по фильтрам

| Filter combination | p99/avg |
|---|---:|
| Selective single-reg (B3, A4, D1) | 1.3–1.8× |
| Batch IN на N=10 | 1.3–2.0× |
| Batch IN на N=100 | 1.2–1.4× |
| flagZeroTurns=false на N=100 | 1.2–1.4× |

Tail предсказуемый: spike'ов нет благодаря трём узлам (cache warm, MAP/REDUCE координируется быстро).

---

## 6. Заметки по индексам и фильтрам

Композитный индекс `IDX_TDC_REG_TYPE_OP (REGISTER, CCTYPEOPER, CCOPERATIONDAY)` оптимален для **всех** реальных WHERE-шейпов:

- A1, A2, A3 (TYPE=50 + диапазон по DAY) — seek по `(REG=?, TYPE=50, DAY range)`
- A4 (TYPE=50 + точный DAY) — exact seek
- B1 (TYPE IN (0,40) + диапазон) — seek по `(REG, TYPE, DAY range)` × 2 значения TYPE
- B2 (TYPE != 50 + диапазон) — может не использовать TYPE из-за NOT EQUAL, fallback на REG+DAY (медленнее)
- C1 (TYPE != 50 + GROUP BY) — то же
- D1 (TYPE != 50 + DAY < ? + ORDER BY CCDATE DESC) — REG seek, потом filter+sort в-памяти
- D2 (TYPE != 50 + DAY range + GROUP BY DAY) — то же

**Слабое место** — `TYPE != 50`. H2 не использует TYPE из индекса для inequality. Если в production это узкое место — рассмотреть отдельный индекс `IDX_TDC_REG_NOT50_DAY` через partial index (Ignite не поддерживает out-of-box, но можно эмулировать через денормализацию `IS_TYPE_50` BOOLEAN).

---

## 7. Чего я НЕ замерил (если нужны цифры — повторный прогон)

| Сценарий | Что покажет |
|---|---|
| `flagZeroTurns=false` отдельно × все N | прямое сравнение пути B vs A |
| `accNum+ucpId` (F2 tuple IN) × N | разница F1 vs F2 |
| `noPeriod=true` × N | сколько rows тянется без `BETWEEN` |
| `pageSize=50, offset=10000` × Total rows = 50,000 | стоимость in-memory skip(10000) |
| CB rates (G1) hits/sec | если валюта != RUB — лишний SQL на каждую запись |
| `noPeriod=true` + N=1000 | worst-case (full register history × 1000 регов) |

Если нужно — поднимаю стенд, добавляю эти 6 шейпов в `RealFiltersPerfController` и прогоняю.

---

## 8. Summary для production

1. **Реальных user filters только 3**: `accountsList`, `filters.{fromDate, toDate, flagZeroTurns}`. Всё остальное (orderBy, pagination) — JVM-only.
2. **flagZeroTurns=true — default и быстрый** путь. Не давать пользователю выбор без причины — false на 27% дороже.
3. **Pagination — ложная**. Тянет все строки, потом режет. Для отчётов >1000 строк нужен **keyset SQL pagination**.
4. **OrderBy — бесплатный** (in-memory). Безопасно давать пользователю любые из 4 опций.
5. **dayBalancesProperties.enabled=true — критично**. Без него выпадает E1/E2 фаза и всё ложится на TURNDOCCUR (×9 медленнее).
6. **noPeriod=true опасен на больших N** — может тянуть всю историю регистра.
7. **accNum+ucpId дороже registerId** для F2 — UI должен сначала найти registerId если возможно (`getStatementSummary` cache layer).

---

## Архивные файлы

- `GETSTATEMENT_SEQUENCE_AND_FILTERS.md` (этот файл) — описание sequence + filters
- `REAL_FILTERS_PERF_REPORT.md` — perf по 13 реальным query-shape'ам
- `FILTERS_PERF_REPORT.md` — старый, **outdated** (фейковые filters: ИНН/счёт/LIKE — не используются в реальности)
- `FINAL_PERF_REPORT.md` — multi-reg perf + сравнение DAYBALANCES vs TURNDOCCUR summary
- `MULTIREG_MULTINODE_REPORT.md` — масштабирование 1..1000 регистров
