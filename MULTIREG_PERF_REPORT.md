# НТ: getStatementSummary — рост N регистров на один request

## Setup

| | |
|---|---|
| Stack | 1 Ignite-узел, Apache Ignite 2.16.0 |
| Volume | 500 регистров × 30 дней × 10 docs = 150K turn-docs, 15K day-balances |
| Indexes | IDX_TDC_REG_TYPE_OP composite + IDX_TDC_OPDAY + IDX_TDC_TYPE + IDX_TDC_IDEKS + IDX_DB_OPDAY |
| Per-register volume | ~300 turn-docs (mass-distributed, не «горячий» register) |
| ANALYZE | прогнан |
| Warmup | 1 iter каждой стратегии, отбрасывается |
| Стратегии (2): | (a) **MULTI-IN** — один SQL с `WHERE REGISTER IN (?,?,…)`, (b) **SPLIT** — N independent SQL по одному регистру каждый |

В каждой итерации одного summary-request'а делается 4 SQL'я (REGISTER+DAYBALANCES+SUM+TYPE-50). Замер per-strategy с p50/p95/p99/max.

## Sweep по N (registersPerRequest)

| N (registers) | MULTI-IN avg | MULTI-IN p99 | SPLIT avg | SPLIT p99 | speedup<br>(multi vs split) |
|---:|---:|---:|---:|---:|---:|
| 1 | 14.4 ms | 29.2 | 15.4 ms | 23.8 | ≈1× |
| 5 | 12.1 ms | 18.1 | 37.5 ms | 59.9 | **3.10×** |
| 10 | 14.5 ms | 32.2 | 48.8 ms | 64.9 | **3.37×** |
| 25 | 33.1 ms | 57.3 | 139.8 ms | 249.7 | **4.23×** |
| 50 | 67.9 ms | 148.8 | 218.4 ms | 263.4 | **3.22×** |
| 100 | 151.0 ms | 198.7 | 412.1 ms | 556.4 | **2.73×** |

## Scaling-анализ

### MULTI-IN: sub-linear до N=10, потом линейно

```
N=1:    14 ms
N=5:    12 ms   (даже быстрее N=1 — IN-list гасит RTT overhead)
N=10:   14 ms   (плато — амортизация RTT)
N=25:   33 ms   (×2.3 от N=10)
N=50:   68 ms   (×4.7 от N=10)
N=100: 151 ms   (×10.4 от N=10)
```

Кривая: `t(N) ≈ 14ms + 1.4 × (N-10) ms` для N > 10. То есть **базовый overhead ~14 ms** (parse + plan + network RTT) + **~1.4 ms на каждый дополнительный register**.

### SPLIT: линейно от N=1

```
N=1:   15 ms
N=5:   37 ms   (~3.7 ms/register * 5 + RTT)
N=10:  49 ms   (~4.9 ms/register * 10)
N=25: 140 ms   (~5.6 ms/register * 25)
N=50: 218 ms   (~4.4 ms/register * 50)
N=100: 412 ms  (~4.1 ms/register * 100)
```

Кривая: `t(N) ≈ N × 4 ms`. Каждая отдельная query тратит **~4 ms** на парсинг + planning + thin-client RTT + index seek.

## Throughput в registers/sec

| N | MULTI-IN req/s | × N regs/req | regs/s | SPLIT req/s | regs/s |
|---:|---:|---:|---:|---:|---:|
| 1 | 69 | 1 | 69 | 65 | 65 |
| 5 | 83 | 5 | **415** | 27 | 134 |
| 10 | 69 | 10 | **690** | 20 | 205 |
| 25 | 30 | 25 | **750** | 7 | 179 |
| 50 | 15 | 50 | **735** | 5 | 229 |
| 100 | 7 | 100 | **662** | 2 | 243 |

**MULTI-IN держит ~700 registers/sec на single thread, независимо от N**.  
**SPLIT — ~200 registers/sec** при любом N — **в 3× меньше**.

## Что это **опровергает** в предыдущих гипотезах

В моих ранних анализах (BUG_REPORT.md, MULTI_IN secrets) я предполагал что multi-IN **деградирует** на больших batch'ах из-за того что H2 декомпозирует IN в N seek'ов. Этот тест **показывает противоположное**: multi-IN **выгодно** на маленьких per-register volume.

### Почему я ошибался

Тот тест с 9-секундным batch'ем (3 регистра в IN) был на **горячем registers с 845k turn-docs каждый**. Бутылочное горлышко там — не количество IN-элементов, а **полный scan огромного per-register range**. На smoke с 300 docs/register ranges крошечные → planner быстро их объединяет в эффективный план.

### Когда что выбирать (актуализированное правило)

| Сценарий | Лучшая стратегия | Почему |
|---|---|---|
| Маленький per-register volume (≤1000 docs) | **MULTI-IN** | RTT-amortization + planner кэширует |
| Большой per-register volume (>50k docs) | **SPLIT** | каждый split идёт через свой optimal index plan; multi-IN может выбрать subptimal |
| Mixed (часть малых, часть огромных) | **SPLIT** | huge ones tank всё в multi-IN |
| Только 1 register | оба равны | overhead amortization уже не важна |

В реальном `getStatementSummary` запросы обычно идут по списку счетов одного клиента — это **5-20 registers**, объём per-register от средне-малого до большого. Гипотеза: **MULTI-IN — правильная стратегия для общего случая**, со SPLIT-fallback для известно-горячих register'ов.

## Возможный гибрид (предлагаю как опцию)

```java
public List<DayBalancesRow> queryBatch(List<String> registers, Date from, Date to) {
    // Сначала pre-check: сколько rows на каждый register
    Map<String, Long> rowCounts = quickEstimate(registers);  // sys.statistics

    List<String> small = registers.stream()
        .filter(r -> rowCounts.getOrDefault(r, 0L) < 10_000)
        .collect(toList());
    List<String> huge = registers.stream()
        .filter(r -> rowCounts.getOrDefault(r, 0L) >= 10_000)
        .collect(toList());

    List<DayBalancesRow> out = new ArrayList<>();
    if (!small.isEmpty()) {
        out.addAll(queryMultiIn(small, from, to));       // одним SQL
    }
    for (String r : huge) {
        out.addAll(querySingle(r, from, to));            // SPLIT для горячих
    }
    return out;
}
```

Эта оптимизация полезна **только если** среди registers попадаются «горячие» с >10k docs. Если все registers маленькие — `queryMultiIn` для всех работает.

## Tail latency

| N | MULTI-IN p99/avg | SPLIT p99/avg |
|---:|---:|---:|
| 1 | 2.0× | 1.5× |
| 10 | 2.2× | 1.3× |
| 50 | 2.2× | 1.2× |
| 100 | 1.3× | 1.4× |

MULTI-IN имеет **более широкий разброс** (p99/avg=2.0-2.2×). Это потому что один из registers в batch'е может оказаться чуть больше других и тянет вверх весь request. SPLIT — узкий разброс (1.2-1.5×) потому что individual queries предсказуемы.

Если SLA жёсткое на p99 — может быть осмысленно использовать SPLIT.

## Выводы

1. **MULTI-IN в 2.7-4.2× быстрее SPLIT** на тестовом volume (300 docs/register). На production-объёмах нужно проверять отдельно.
2. **Throughput на single thread**: MULTI-IN ~700 regs/sec vs SPLIT ~200 regs/sec.
3. **Кривая роста MULTI-IN**: 14 ms base + ~1.4 ms/register linear (до N=100).
4. **SPLIT линейный с самого начала**: ~4 ms × N.
5. **Tail latency**: MULTI-IN шире (2.2×), SPLIT уже (1.4×). Если SLA на p99 — SPLIT может быть предпочтительнее.
6. **Гипотеза опровергнута**: моё предыдущее утверждение «multi-IN деградирует» применимо только к крупным registers (≥50k docs each). На массовых registers с короткой историей multi-IN выигрывает.

## Recommendation для GetStatementSummaryLibraryIgnite

Сохранить **MULTI-IN** стратегию для general-case (5-20 registers, mixed sizes). Добавить **fall-back на SPLIT** через config flag или pre-check:

```yaml
get-statement-summary:
  query-strategy: multi-in   # multi-in | split | hybrid
  hybrid-row-threshold: 10000   # registers с rowCount > threshold идут через split
```

В коде хранить два метода и выбирать в runtime через `IgniteClusterPool.execute()`.

## Endpoint для воспроизведения

```bash
# Sweep по N
for n in 1 5 10 25 50 100; do
  curl -X POST "http://localhost:18080/api/perf/summary-multi-reg?registersPerRequest=$n&iterations=20"
  echo
done
```

`POST /api/perf/summary-multi-reg?registersPerRequest=N&iterations=K` — возвращает statsOf для MULTI-IN и SPLIT + speedup ratio.

Source: `GetStatementSummaryPerfController.summaryMultiReg()`.
