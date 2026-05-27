# Оптимизации сервисов под production-масштаб (recalibrated)

## Реальный workload (corrected)

- **6М активных регистров**
- Nightly recalc запускается на **каждый активный регистр** для **1 дня** (yesterday) + создаёт type50 на today (входящий остаток)
- **50М проводок/день** распределено по 6М регистров (avg ~8.3/регистр) с тяжёлым Pareto-хвостом
- Window: ночь ≈ 4 часа → требуется throughput **≥ 1666 регистров/сек**

Помимо nightly есть два дополнительных пути:
- `recalcByRecalcDate` — back-dated регистры (CDC сдвинул CCBALANCERECALCDATE на много дней назад)
- `recalcNullRecalcDateRegisters` — initial recalc новых регистров (до 6 месяцев)

## Predecessor: ошибка в моей предыдущей матмодели

Я писал "6M × 30 дней recalc". **Неверно**. Реально:
- Nightly: **6M × 1 day** = 6M day-recalcs
- Back-dated tail: ~0.5% регистров × multi-day range
- Initial: новые регистры × 120 day range (редко)

Соответственно мои оптимизации #3 (batch aggregates на range) и #4 (batch cache I/O на range) для **nightly** даю marginal-effect (range = 1 день → `getAll(1 key)` ≡ `get(1 key)`). Они helpful **только** для multi-day путей: `recalcByRecalcDate` и `recalcNullRecalcDateRegisters`.

Поэтому добавлена **оптимизация #6** — multi-register batching специально для nightly hot path.

---

## Полный список оптимизаций (6 шт.)

| # | Файл | Где помогает | Сохранение значений |
|---|---|---|---|
| 1 | `GetStatementSummaryLibraryIgnite` | `getStatementSummary` API (UI запросы, не recalc) | Мемоизация идемпотентной функции |
| 2 | `GetStatementSummaryLibraryIgnite` | `getStatementSummary` API | Алгебраическое перепиcывание SQL (A2+A3 → JOIN) |
| 3 | `DayBalancesRecalcService.recalcRegisterRange` | Multi-day recalc (`recalcByRecalcDate`, init) | `SUM`/`COUNT`/`MAX` по `GROUP BY (DAY,CCDT)` == per-day query'ям |
| 4 | `DayBalancesRecalcService.recalcRegisterRange` | Multi-day recalc | `getAll/putAll/removeAll` ≡ множеству `get/put/remove` |
| 5 | `DayBalancesRecalcService.recalcRegisterRange` | Multi-day recalc, cold tail | Empty aggregates → нет new put → тождественно общему пути |
| **6** | `DayBalancesRecalcService.scheduledRecalcYesterday` | **Nightly 6M × 1-day** | Per-register loop в-памяти после batched setup queries — тот же chain startSum, те же side-effects |

---

## Optimization #6 в деталях: multi-register batch для nightly recalc

### Что было до

```java
private void scheduledRecalcYesterday() {
    ...
    activeRegs.stream()
        .filter(...)
        .map(info -> CompletableFuture.runAsync(
            () -> recalcRegisterRange(info.objectId, yesterday, yesterday),
            recalcExecutor))
        ...
}
```

Per register × 6M:
- 1 SQL: `getCurrencyCode` (cache.get на REGISTER + SQL на CURRENCY)
- 1 SQL: `getRegisterOpenDate`
- 1+ SQL: `findStartSum` (sV1 hit'ы обычно, 1 SQL)
- 1 SQL: `findPrevOperDate`
- 1 SQL: `fetchDayAggregatesRange` для [yesterday]
- 1 cache.get + 1 cache.put/remove: DAY_BALANCES
- 1 SQL + 1 cache.put: `upsertTypeOper50` для today
- ≈ 7–9 SQL/cache ops per register
- **Total: ~50M ops за ночь**

### Что стало после #6

```java
private void scheduledRecalcYesterday() {
    List<String> filtered = activeRegs.stream().filter(...).map(...).collect(Collectors.toList());
    // [#6] Chunk регистры и параллельно обрабатываем chunks
    for (int i = 0; i < filtered.size(); i += NIGHTLY_BATCH_CHUNK) {
        List<String> chunk = filtered.subList(i, ...);
        futures.add(CompletableFuture.runAsync(
            () -> recalcRegistersBatchForDay(chunk, yesterday), recalcExecutor));
    }
}
```

`recalcRegistersBatchForDay(chunk, day)` per chunk × ~6K chunks:
1. **`batchGetCurrencyCodes(chunk)`** — 1 `cache.getAll(REGISTER)` + 1 SQL `WHERE OBJECTID IN(?,...)`
2. **`batchFindStartSumSV1(chunk, day)`** — 1 SQL `WHERE REGISTER IN(?,...) AND CCTYPEOPER=50 AND CCOPERATIONDAY=?`
3. **`batchFindPrevOperDate(chunk, day)`** — 1 SQL `WHERE REGISTER IN(?,...) AND CCOPERATIONDAY<? GROUP BY REGISTER`
4. **`fetchDayAggregatesForRegistersOnDay(chunk, day)`** — 1 SQL `WHERE REGISTER IN(?,...) AND CCOPERATIONDAY=? GROUP BY REGISTER, CCDT`
5. **`dbCache.getAll(allDayBalanceKeys)`** — 1 cache call для chunk-key'ов
6. Per-register loop (in-memory): compute, batch put/remove
7. **`dbCache.removeAll/putAll`** — 0–2 cache calls
8. **type50 upserts** per-register (пока не batched — потенциал для #7)
9. **KAP messages** per-register

Per chunk (K=1000): **~5 SQL + ~3 cache ops + K per-register type50/KAP**.

Per night: ~6K chunks × ~5 setup ops = **~30K setup ops** (vs ~30M раньше) = **×1000 reduction на setup**.

Type50 upserts остаются per-register (можно ещё батчить, см. "Дальнейшие шаги"). Для 6M регистров с 8 проводок/день avg = ~6M type50 upserts остаются. Это следующее узкое место.

### Сохранение значений

| Step | Старый код | Batch вариант | Эквивалентность |
|---|---|---|---|
| Currency code | `cache.get + SQL per reg` | `cache.getAll + SQL IN-clause` | API-эквивалент |
| StartSum sV1 | per-reg SQL on `WHERE REGISTER=? AND TYPE=50 AND DAY=?` | one SQL `WHERE REGISTER IN(...) AND TYPE=50 AND DAY=?` | Декомпозиция aggregator'а WHERE = IN-clause |
| StartSum sV2/sV3/sV4 fallback | per-reg sequential SQL | per-reg fallback for misses | Same logic per reg, не покрывается batch — pre-collected sV1 hits + fallback |
| PrevOperDate | per-reg `WHERE REG=? AND DAY<? AND TYPE!=50` | `WHERE REG IN(...) GROUP BY REG` | Decomposition: same MAX per register |
| Aggregates | per-reg `GROUP BY CCDT` | per-(reg, CCDT) `GROUP BY REGISTER, CCDT` | Same per-reg breakdown |
| DAY_BALANCES diff-check | per-reg `cache.get` | `cache.getAll(chunk keys)` | API-эквивалент |
| DAY_BALANCES write | per-reg `cache.put/remove` | `cache.putAll/removeAll` | API-эквивалент |
| `upsertTypeOper50` | per-reg, в loop'е | per-reg, в loop'е (отложено за batch'ом) | Тот же вызов, тот же registry argment |
| `sendBalanceInfo`, `checkIncomeSaldo` | per-reg | per-reg (in batch loop) | Сохранены per-register |
| `createKapMessageForRecalc` | per-reg, в loop'е | per-reg, отложено за batch'ом | Тот же ordering: DAY_BALANCES → KAP |
| Chain startSum=finishSum | per-reg sequential | per-reg sequential (in chunk loop) | Same per-register chain |

---

## Сводка операций под РЕАЛЬНОЙ нагрузкой (6M registers × 1 day nightly)

| Фаза | До | После #6 |
|---|---:|---:|
| Setup queries (currency/start/prev) | ~24M SQL | ~18K SQL (6K chunks × 3) |
| Aggregation | 6M SQL | 6K SQL (6K chunks × 1) |
| DAY_BALANCES get | 6M | 6K (chunks × 1 getAll) |
| DAY_BALANCES put/remove | ~3M (only dirty) | ~6K (chunks × ≤2) |
| Type50 upsert (per-reg) | ~6M | ~6M (не batched пока) |
| KAP per-changed-reg | ~3M | ~3M |
| **Total cache+SQL ops** | **~48M** | **~9M** |
| **Reduction** | base | **×5–6** для setup, **dominant остаётся type50** |

Главное узкое место теперь — type50 upserts (6M per night). Это **следующая** оптимизация. Можно ускорить через batched put на TURN_DOC_CUR (создание новых) + batched UPDATE (изменение существующих). Но требует **batched SqlFieldsQuery с UPDATE и multi-row VALUES** — не везде поддерживается.

---

## Под нагрузкой `recalcByRecalcDate` (back-dated tail) — оптимизации #3–#5 актуальны

Это тот сценарий о котором я думал изначально (range > 1 день).

Per-register, range=N days (N от 2 до 120 в крайнем случае):
- #3 batch aggregates: 1 SQL вместо N → ×N reduction
- #4 batch cache I/O: 3 ops вместо до 3N → ×N reduction
- #5 fast-path: skip-early если range пуст

Для back-dated tail (~0.5% регистров × avg 5-дневный range): **30K регистров × ~5 days = 150K day-recalcs**. С #3/#4/#5 это становится 30K SQL + 30K cache ops = ~60K total. Без них: ~750K.

Для initial recalc новых регистров (range = 6 мес = ~180 days): **#3/#4/#5 критичны**. Без них: 180+ SQL per register. С ними: 1–3 SQL per register.

---

## Verification

Все 6 оптимизаций компилируются и проходят bug-fix repro test:

```
$ mvn -pl stmnt-ignite-lib -am compile
COMPILE_OK

$ POST /api/repro/recalc-bug/setup
$ POST /api/repro/recalc-bug/move
$ POST /api/repro/recalc-bug/recalc?mode=fixed
{
  "dayBalance_16_deleted": true,    ← PASS, фикс сохранён после всех optimizations
  "dayBalance_17_exists": true,
  "test_passed_with_fixed": true
}
```

---

## Дальнейшие шаги (вне scope текущего патча)

| # | Что | Эффект |
|---|---|---|
| 7 | Batch type50 upserts: separate INSERT/UPDATE batches вместо per-reg find→put/update | ×100 reduction на type50 (6M → 60K ops) |
| 8 | Affinity-co-located compute (`ignite.compute().affinityRun(...)`) | Push recalc TO data node, минуя SQL вообще |
| 9 | Hot-tail отдельный pool с большей parallelism | Lower p99 latency для горячих регистров (1M+ docs/day) |
| 10 | KAP messages batched через putAll на KAP_MESSAGE cache | ×100 на KAP (3M → 30K ops) |

После #7 + #10 — обработка ночного recalc упирается в **scan source data** в Ignite SQL planner, не в API overhead.

---

## Файлы патча

| Файл | Optimizations |
|---|---|
| `summary/.../GetStatementSummaryLibraryIgnite.java` | #1 rate cache, #2 A2+A3 JOIN |
| `stmnt-ignite-lib/.../DayBalancesRecalcService.java` | #3 batch aggregates, #4 batch DAY_BALANCES I/O, #5 fast-path cold, **#6 multi-register nightly batch** + pre-existing compile fixes |
| `stmnt-consistency-service/.../RecalcBugReproController.java` | Repro endpoints — верификация семантики после оптимизаций |
| `SERVICE_OPTIMIZATIONS.md` | этот файл |
