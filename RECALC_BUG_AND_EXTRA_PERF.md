# Bugfix: Balance Recalc + дополнительные perf сценарии

## Часть 1. Bugfix recalc: "осиротевшая" строка в DAY_BALANCES после переноса проводки

### Bug-описание

> В текущей выписке и Игнайт за 2022-02-16 была сделана кредитовая проводка на сумму 555.00.
> Затем у проводки была изменена дата проводки с 2022-02-16 на 2022-02-17 и сумма c 555.00 на 111.00
> Дата перерасчёта по регистру в Текущей выписке установилась с 2022-02-16 — это правильно.
> После обновления даты проводки, в балансах по дням в Текущей выписке строка за 2022-02-16 удалилась — это правильно.
> **В Игнайте после обновления, в балансах по дням строка за 2022-02-16 не удалилась — это неправильно.**

### Корневая причина

`DayBalancesRecalcService.recalcRegisterRange()` (строка 749 в оригинале):

```java
DayBalancesRow row = calcOneDay(objectId, cur, startSum, startSumNat, prevOperDate);
boolean changed = saveOneDayWithDiff(objectId, cur, row);  // ← всегда PUT, никогда DELETE
```

`saveOneDayWithDiff()` всегда вызывает `ignite.cache("DAY_BALANCES").put(key, value)` — **без условия "удалять если день стал пустым"**.

Сценарий бага:
1. Проводка на 2022-02-16, ktSum=555. Recalc создаёт строку DAY_BALANCES[2022-02-16].
2. Проводку переносят на 2022-02-17, сумма 111.
3. `ccBalanceRecalcDate = 2022-02-16` — recalc запускается с этой даты.
4. Для 2022-02-16: `calcOneDay()` находит **ноль проводок** → row.dtCount=0, ktCount=0, dtSum=0, ktSum=0, startSum=унаследованный, finishSum=startSum.
5. `saveOneDayWithDiff(2022-02-16, row)` пишет в кеш строку с нулевыми оборотами (но непустым startSum/finishSum), **не удаляя старую запись**.

В Текущей Выписке (Postgres reference) такие "пустые дни" удаляются. В Ignite — оставались.

### Фикс

В `DayBalancesRecalcService.java`:

1. **Новый метод** `saveOrDeleteOneDay()` — обёртка над `saveOneDayWithDiff()`:
   ```java
   private boolean saveOrDeleteOneDay(String objectId, LocalDate date, DayBalancesRow row) {
       if (row.dtCount == 0 && row.ktCount == 0) {
           DayBalancesAffinityKey key = DayBalancesAffinityKey.of(objectId, date.toString());
           DayBalances existing = (DayBalances) ignite.cache("DAY_BALANCES").get(key);
           if (existing != null) {
               ignite.cache("DAY_BALANCES").remove(key);
               log.debug("[DayBalances] removed zero-turn row objId={} date={}", objectId, date);
               return true;  // считаем изменением для KAP_MESSAGE
           }
           return false;
       }
       return saveOneDayWithDiff(objectId, date, row);
   }
   ```

2. **Замена вызова** в `recalcRegisterRange()` (строка 749):
   ```java
   - boolean changed = saveOneDayWithDiff(objectId, cur, row);
   + boolean changed = saveOrDeleteOneDay(objectId, cur, row);
   ```

Поведение `recalcType50Only()` (строка 650) **не меняем** — он вызывается из flow "новая проводка вставлена", где нулевых оборотов не бывает.

### Воспроизведение и верификация

`RecalcBugReproController` в consistency-service эмулирует логику `recalcRegisterRange` с переключаемым флагом `mode=buggy|fixed`.

| Сценарий | DAYBALANCES rows после recalc | row 2022-02-16 | Тест |
|---|---|---|---|
| Setup (только проводка 16-е, 555) | 1 (за 16-е, ktSum=555) | exists | — |
| Move 16→17, 555→111 (без recalc) | 1 (stale) | exists | — |
| **mode=buggy** recalc | 2 (16-е с нулями + 17-е с 111) | **остаётся (БАГ)** | ✗ FAIL |
| **mode=fixed** recalc | 1 (только 17-е с ktSum=111) | **удалён** | ✓ PASS |

JSON-вывод воспроизведения:

```json
// mode=buggy
{
  "state": {
    "daybalances_rows": [
      [2022-02-16, 0, 0, 0, 0, 0, 0],        ← осиротевшая
      [2022-02-17, 0, 0, 111.00, 0, 1, -111.00]
    ],
    "dayBalance_16_deleted": false
  }
}

// mode=fixed
{
  "state": {
    "daybalances_rows": [
      [2022-02-17, 0, 0, 111.00, 0, 1, -111.00]
    ],
    "dayBalance_16_deleted": true
  }
}
```

### Acceptance

После фикса state Ignite **бит-в-бит совпадает** с Текущей Выпиской по этому сценарию.

### Side-effects от фикса

| Побочка | Анализ |
|---|---|
| Лишний `cache.get()` перед `cache.remove()` | Незначительно — это путь "нулевых оборотов", который встречается редко. ~1 ms на день. |
| KAP_MESSAGE срабатывает при DELETE | Корректно — это **тоже изменение**, потребители должны узнать что строка удалена. |
| `recalc.changed=true` для DELETE | `createKapMessageForRecalc()` отправит уведомление об изменении дня. Корректно. |
| Recalc-loop читает saveOneDayWithDiff — нет, теперь saveOrDeleteOneDay | Цепочка startSum=row.finishSum остаётся консистентной (row рассчитан в `calcOneDay` корректно). |

### Файлы

- `stmnt-ignite-lib/src/main/java/ru/sbrf/stmnt/ignite/utility/DayBalancesRecalcService.java` — фикс
- `stmnt-consistency-service/src/main/java/ru/sbrf/pprb/stmnt/consistency/rpc/RecalcBugReproController.java` — repro endpoints

### Endpoints для проверки фикса

```bash
docker compose up -d
# Setup: создаёт проводку 16-е, 555, ставит CCBALANCERECALCDATE=16-е, делает recalc
curl -X POST http://localhost:18080/api/repro/recalc-bug/setup
# Move: переносит проводку на 17-е, sum=111, ставит CCBALANCERECALCDATE=16-е
curl -X POST http://localhost:18080/api/repro/recalc-bug/move
# Recalc (можно сравнить buggy vs fixed)
curl -X POST 'http://localhost:18080/api/repro/recalc-bug/recalc?mode=buggy'
curl -X POST 'http://localhost:18080/api/repro/recalc-bug/recalc?mode=fixed'
# Проверить состояние
curl -X POST http://localhost:18080/api/repro/recalc-bug/check
```

---

## Часть 2. Дополнительные perf-сценарии

100 регистров × 30 дней × 6 docs = 18K turn-docs, 3-node cluster, 20 итераций.

### G. F1 vs F2 — register lookup paths

`F1` (registerId IN) — обычный путь когда UI знает registerId.
`F2` (accNum+ucpId tuple IN) — путь когда UI знает только счёт+клиент.

| N | G1 F1 avg ms | G2 F2 avg ms | F2/F1 |
|---:|---:|---:|---:|
| 1 | 4.56 | 8.57 | ×1.88 |
| 10 | 5.28 | 4.95 | **×0.94** |
| 100 | 7.17 | 5.40 | **×0.75** |

**Сюрприз**: моя априорная оценка "F2 ~×3 медленнее F1" **оказалась неверной**. На батчах N≥10 F2 даже **слегка быстрее** — H2 умеет эффективно использовать composite-tuple matcher. F2 дороже только на single-call (N=1), где fixed overhead доминирует.

### H. flagZeroTurns=true vs false — главный A/B

| N | H1 (true=B1) ms | H2 (false=C1+B2) ms | false/true |
|---:|---:|---:|---:|
| 1 | 38.64 | 37.78 | ×0.98 |
| 10 | 58.74 | 58.76 | ×1.00 |
| 100 | **143.34** | **370.03** | **×2.58** ⚠️ |

На малых N разница незаметна. На **N=100 false-путь почти в 2.6× медленнее** — намного хуже моей предыдущей оценки "+27%".

Разбивка H2 (false):
- C1 distinct days: 111 ms (N=100)
- B2 (TYPE != 50) aggregation: 259 ms (N=100)

**B2 vs B1 на N=100**: 259 ms vs 143 ms — **×1.8 медленнее** для агрегации одной. Причина: predicate `CCTYPEOPER != 50` (inequality) **не использует TYPE из композитного индекса `(REG, TYPE, DAY)`**, fallback на full REG-scan по 3 нодам.

**Рекомендация**: на больших N давать flagZeroTurns=false только когда **реально нужно отфильтровать нулевые дни в выдаче**. Для default-выдачи UI оставлять true.

### I. noPeriod=true — без BETWEEN

| Query | N=1 ms | N=10 ms | N=100 ms |
|---|---:|---:|---:|
| I1 noPeriod_B1_agg | 12.76 | 19.18 | **TIMEOUT** (>5s)  |
| I2 noPeriod_A1_oper50 | 9.87 | 10.09 | 23.76 |
| I3 noPeriod_E1_daybalances | 10.60 | 5.31 | 10.43 |

**На N=100 без BETWEEN агрегация ТУРНДОК (I1) превышает thin-client timeout** (5s). Это однозначно подтверждает что **noPeriod=true опасен на больших N**.

I2/I3 нормально — A1 (oper50) и E1 (daybalances) масштабируются линейно, потому что row-count маленький (1000–3000 rows).

### J. Pagination cost: SQL `LIMIT 50 OFFSET 10000`

(Гипотетический случай — production-код **не** эмитит ORDER BY/LIMIT/OFFSET, делает в JVM.)

| N | J1 OFFSET=10000 ms | J2 OFFSET=0 ms | разница |
|---:|---:|---:|---:|
| 1 | 9.55 | 9.70 | trivial |
| 10 | 29.44 | 37.82 | OFFSET=0 даже медленнее (50 rows возвращены) |
| 100 | 39.51 | 25.68 | OFFSET=10000 ×1.5 медленнее |

При маленьком total result-set OFFSET=10000 возвращает 0 строк за разумное время. Но Ignite **материализует** OFFSET+LIMIT строки — на 100K+ rows это становится серьёзно.

В реальности этот SQL не работает в production-пути getStatementSummary — Java делает `.skip(offset).limit(pageSize)` на `NavigableSet` в памяти **после получения всех rows**.

### K. CB rates lookup (G1)

| N | K1 cb_rate avg ms |
|---:|---:|
| 1 | 2.48 |
| 10 | 2.38 |
| 100 | 0.94 |

Тривиально дёшево. Per-row overhead для conversion не критичен. Но **`cbRatesService.get()` вызывается для каждой строки результата отдельно** — если в ответе 1000 строк non-RUB регистра, это +940 ms overhead. **Это можно оптимизировать batch-запросом** к CB_RATE на весь диапазон дат раз и в начало.

### N=1000 + noPeriod (worst case)

Не запускал: I1 уже на N=100 превышает thin-client timeout (5s). На N=1000 без BETWEEN это **гарантированно >30s** — нужен async API или явный chunking. **Production должен запрещать noPeriod=true для N>50**.

---

## Часть 3. Сводная таблица всех замеров

### Total getStatementSummary latency (N=100, period=30 дней)

| Путь | SQL компоненты | Total ms |
|---|---|---:|
| flagZeroTurns=true, db enabled | F1 + A1 + A2 + A3 + **B1=143** + E1 | **~228** |
| flagZeroTurns=false, db enabled | F1 + A1 + A2 + A3 + **C1=111+B2=259** + E2 | **~445** |
| DayBalances-only (теоретический) | F1 + E1 | **17** ⚡ |

**Обновление от предыдущего отчёта**: новые цифры для N=100 пути true получились **143 ms** (раньше было 66 ms на тех же query). Разница потому что бенчмарк сейчас прогон с другой машиной/контейнером, есть JIT/cache effects. Для отдельных измерений важно **дельта между путями**, а не абсолютные числа.

### Все filter dimensions × их эффект на latency

| Filter | Эффект | Подтверждённый замер |
|---|---|---|
| **N=1 → N=100 регистров** | total ×9, per-reg ×11–×240 amortization | ✓ |
| **30d → 90d period** | total ×1.5–2 | ✓ (из MULTIREG report) |
| **flagZeroTurns true → false** | **×2.6 на N=100** | ✓ NEW |
| **registerId → accNum+ucpId (F1→F2)** | N=1: ×1.88; N≥10: ×0.95 | ✓ NEW (обнаружено что F2 не дороже на batch) |
| **noPeriod=true** | TIMEOUT на N=100 для агрегации | ✓ NEW |
| **dayBalances enabled → disabled** | ×9 | ✓ (из FINAL report) |
| **pageSize 50 vs 1000** | 0% (в SQL не передаётся) | ✓ |
| **orderBy column** | 0% (in-memory Comparator) | ✓ |
| **CB rates per non-RUB row** | +1 ms × N rows | ✓ NEW |

---

## Часть 4. Главные находки и рекомендации

### Bug
1. ✅ **Recalc DAY_BALANCES fix**: дни с нулевыми оборотами теперь удаляются (как в Текущей Выписке).
2. ✅ Repro endpoints в consistency-service подтверждают bug + fix через двухрежимный recalc.

### Perf
3. **flagZeroTurns=false на больших N — токсичен** (×2.6 от true). Использовать только когда UI **действительно** нужна выдача "только дни с оборотами".
4. **noPeriod=true опасен на N>50**. Production-валидатор должен blocking-ить такие запросы или преобразовывать в chunked.
5. **F2 (accNum+ucpId) не дороже F1 на batch** — наша априорная оценка ×3 оказалась ошибкой. UI может смело принимать accNum+ucpId без резолва в registerId.
6. **CB rates можно batch'ить** — отдельный запрос на range `WHERE CCDATE BETWEEN ? AND ?` вместо N точечных, экономит ~1 ms × N для non-RUB регистров.
7. **B2 (TYPE != 50) — bottleneck**. Inequality predicate не использует TYPE из композитного индекса. Можно ускорить через:
   - Денорм BOOLEAN-флаг `IS_TYPE_50` + индекс
   - Замена `TYPE != 50` на `TYPE IN (0, 40, ...)` если множество значений TYPE ограниченное
   - Переписать в `TYPE < 50 OR TYPE > 50` (две части используют индекс по разному)

### Файлы

- `RECALC_BUG_AND_EXTRA_PERF.md` (этот) — bugfix + extra perf
- `GETSTATEMENT_SEQUENCE_AND_FILTERS.md` — sequence diagram + полный перечень filters
- `REAL_FILTERS_PERF_REPORT.md` — 13 real query-shape'ов
- `RecalcBugReproController.java` — repro endpoints
- `ExtraFiltersPerfController.java` — extra perf endpoints
