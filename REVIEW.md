# Массовое ревью проекта

**Скоуп:** `stmnt-consistency` (Dropapp-сервис сверки 3 Ignite-кластеров) + `stmnt-ignite_precalc` (изменения в `DayBalancesRecalcService.java`) + docker-compose стенд + документация.

**Дата:** 2026-05-26.

**Метод:** комбинированный — code-review агента на `stmnt-consistency-service`, моё ревью на Ignite-side и операционные аспекты.

---

## Сводка по серьёзности

| Уровень | Кол-во | Категории |
|---|---:|---|
| **CRITICAL** | 6 | unauth backdoor, SQL injection, hang без timeout, no reconnect, double-call |
| **MAJOR** | 16 | memory unbounded, batch sizes, thread pools, transactions, race conditions |
| **MINOR** | 22 | API consistency, error messages, magic numbers |
| **NIT** | 18 | стиль, дубликаты, JavaDoc |
| **Strengths** | 14 | хорошие архитектурные решения |

---

## 1. CRITICAL (исправлять немедленно перед prod)

### C1. `/api/debug/exec` — unauth arbitrary SQL backdoor
**Файл:** `consistency-service/.../rpc/DebugSeedController.java:284-304`
Endpoint принимает любой SQL (`DROP TABLE`, `DELETE`, ...) и выполняет через thin client. Никакой Spring Security в проекте нет. **RCE-эквивалент против Ignite-кластеров**.
**Фикс:** `@Profile("!prod")` или удалить. То же для `/api/debug/seed`, `/scenario/*`, `/reset`, `/analyze-all`, `/explain`, `/explain-all`, `/affinity`.

### C2. SQL injection by design в `/api/debug/exec`
**Файл:** `DebugSeedController.java:293`
SQL из тела запроса конкатенируется в `SqlFieldsQuery` без какой-либо валидации. **Это не "уязвимость" — это документированное поведение** debug-endpoint'а, но в prod недопустимо.
**Фикс:** см. C1.

### C3. `IgniteClientFactory` без retry / reconnect
**Файл:** `consistency-service/.../integration/ignite/IgniteClientFactory.java:30-48`
Если кластер недоступен на старте, `clients.get(id)` возвращает `null` навсегда. Восстановление — только рестарт сервиса. Для 24/7 мониторинга это серьёзная дыра.
**Фикс:** scheduled reconnect через `lazy supplier` или wrapper, который пересоздаёт `IgniteClient` при `null`.

### C4. `ConsistencyJob.processOneCache` — `future.join()` без timeout
**Файл:** `consistency-service/.../lib/ConsistencyJob.java:112-114`
Per-query `setTimeout(60_000)` (ClusterReader:83) ограничивает только SQL — не TCP-stalls. Зависший connect перевешает весь часовой прогон.
**Фикс:** `future.get(timeout, TimeUnit.MILLISECONDS)` + cancel + log+errorRegistry.

### C5. Hash non-determinism в TurnDocCurHasher для CCTYPEOPER
**Файл:** `consistency-service/.../lib/hash/Hashers.java:90`
`((BigDecimal) asBigDecimal(typeOperObj)).intValueExact()` бросает на fractional value (например `50.0` хранится по-разному); ловится только общим catch в `ClusterReader.tryRunInto` → весь cache marked as `QUERY_FAILED` для одной кривой строки.
**Фикс:** try/catch вокруг row processing, skip-on-error с логированием в `ErrorRegistry`.

### C6. Двойной вызов `loadRegisterSnapshot` в `initRegister` (Ignite-side)
**Файл:** `stmnt-ignite_precalc/.../DayBalancesRecalcService.java:939-940`
```java
String currencyCode = loadRegisterSnapshot(registerId) != null
        ? loadRegisterSnapshot(registerId).currencyCode
        : RUBLE_CODE_643;
```
Два cache-get'а подряд на REGISTER. Между ними возможно изменение (хотя бы в init на 180 дней). Логически ОК (мы читаем одно и то же поле), но это явный bug: 2× обращений + race window.
**Фикс:**
```java
RegisterSnapshot snap = loadRegisterSnapshot(registerId);
String currencyCode = snap != null ? snap.currencyCode : RUBLE_CODE_643;
```

---

## 2. MAJOR (важно для production)

### M1. `ClusterReader.computeHashes` — full `HashMap` в RAM
**Файл:** `ClusterReader.java:38, 45-50`
Для `TURN_DOC_CUR` (lookback 3 дня × 10M записей/день × 3 кластера × 9 кэшей параллельно) — сотни МБ. Нет streaming/chunking.
**Фикс:** хешировать чанками `OBJECTID % N`, или пагинация по `CCOPERATIONDAY` день за днём.

### M2. Union ВСЕХ keys в `processOneCache` — multiplied OOM
**Файл:** `ConsistencyJob.java:122-139`
`allKeys = union(...)` + `hashesForKey` per key + `detected.add(...)`. При схема-mismatch миллионы строк в `consistency_mismatch`.
**Фикс:** cap на `detected.size()` (например 10k); при превышении — fast-fail run со статусом `MISMATCH_OVERFLOW`.

### M3. `JdbcHashRepository.saveBatch` — unbounded batch
**Файл:** `JdbcHashRepository.java:18-27`
Миллион строк в одном `jdbc.batchUpdate` → blow PG memory + prepared-statement cache.
**Фикс:** chunk by 1-5k через `Lists.partition` или ручной цикл.

### M4. `JdbcMismatchRepository.saveBatch` — то же + per-row Jackson
**Файл:** `JdbcMismatchRepository.java:29-41`
**Фикс:** аналогично + pre-size `ArrayList`.

### M5. Cached unbounded ThreadPools
**Файлы:** `ConsistencyJob.java:47` (`clusterPool`), `AdminFanOut.java:37` (`pool`)
`Executors.newCachedThreadPool` без верхнего предела. Сегодня OK для 3 кластеров, но REST-spam взорвёт.
**Фикс:** `newFixedThreadPool(props.getClusters().size())` или bounded `ThreadPoolExecutor` с `LinkedBlockingQueue` + `CallerRunsPolicy`.

### M6. `IgniteClientFactory.clients` — не thread-safe `LinkedHashMap`
**Файл:** `IgniteClientFactory.java:28`
Читается из cluster-pool threads. JLS happens-before гарантирован только для `final`/`volatile`.
**Фикс:** `ConcurrentHashMap<>` или `Collections.unmodifiableMap(...)` после init.

### M7. `ErrorRegistry.record` не полностью infallible
**Файл:** `ErrorRegistry.java:36-46`
Контракт `"infallible"` — но если `mapper.writeValueAsString(details)` бросит `JsonMappingException` (наследник `IOException`) или прилетит `RuntimeException` от `details.entrySet().iterator()` (например, custom `Map` с side-effect) — это не поймано.
**Фикс:** обернуть весь body method'а в try/catch с лог в slf4j.

### M8. `mismatches.saveBatch` без транзакции
**Файл:** `ConsistencyJob.java:140`
`hashStore.saveBatch` + `mismatches.saveBatch` в одной логической операции — partial state при падении одной.
**Фикс:** `@Transactional` на `processOneCache` или wrap save'ов.

### M9. `Instant.parse(since)` без validation
**Файл:** `ConsistencyController.java:50`, `ErrorController.java:28`
Невалидный ISO → 500 со stack trace.
**Фикс:** try/catch + 400 Bad Request с message.

### M10. `ConsistencyController.run` синхронный
**Файл:** `ConsistencyController.java:24-30`
Полный прогон может идти минуты-часы → HTTP timeout, повторные firings.
**Фикс:** `@Async` + сразу вернуть `runId`, polling через `/runs/{id}`.

### M11. Race в `DayBalancesRecalcService.dispatchDebouncedRecalc` (Ignite-side)
**Файл:** `DayBalancesRecalcService.java:dispatchDebouncedRecalc` (lines ~580-610)
`pendingRecalc.remove(registerId)` + `pendingOperDay.remove(registerId)` + `pendingStorno.remove(registerId)` — 3 операции **не атомарные**. Если между ними новое событие пришло, `pendingOperDay` уже null, а `pendingStorno` ещё может содержать старое значение.
**Фикс:** snapshot всех 3 значений за одной транзакцией (через `compute`), или собрать их под общим `lock`.

### M12. Нет defensive copy в `Hashers.queryParams()`
**Файл:** `Hashers.java:84, 163`
`LocalDate.now()` пересчитывается каждый вызов. Между чтениями 3 кластеров запрос может «перескочить» полночь — разные cutoff dates у разных кластеров.
**Фикс:** snapshot `LocalDate.now()` в `ConsistencyJob.processOneCache` и пробросить вниз через context-param.

### M13. `DayBalancesRecalcService` `synchronized(lockKey.intern())` — устарел
**Ignite-side**: ранее в проекте использовался `String.intern()` для блокировок. После моих правок остались `ReentrantLock UPSERT_LOCKS[1024]` через `lockFor(...)` — правильно. Но `cqOffloadExecutor` имеет `DiscardOldestPolicy` (line 110) — события могут теряться при шторме.
**Фикс:** заменить на `CallerRunsPolicy` или scale capacity (`new LinkedBlockingQueue<>(500_000)`).

### M14. `DebugSeedController.seed` swallows per-cluster errors
**Файл:** `DebugSeedController.java:52-62`
`applyScenario` → вызывает `seed(Map.of())` → если baseline seed fail частично, scenario продолжает работать с возможно-пустыми таблицами → невалидный тест.
**Фикс:** fail-fast, если seed не отработал на всех кластерах.

### M15. `consistency_error` без retention
**Файл:** `V3__error_registry.sql`
В отличие от `consistency_run` (есть `purgeOldHashes`), для ошибок retention не настроен.
**Фикс:** добавить cron-task в `ConsistencyJob`:
```sql
DELETE FROM consistency_error
 WHERE resolved_at IS NOT NULL AND resolved_at < now() - INTERVAL '90 days'
```

### M16. `ConsistencyJob.scheduledRun()` без overlap protection
**Файл:** `ConsistencyJob.java:53`
Если предыдущий run занял > 1 час (например, на большом TURN_DOC_CUR), Spring запустит **второй параллельно**. Двойная нагрузка на кластера.
**Фикс:** `@SchedulerLock` (ShedLock library) или флаг `AtomicBoolean isRunning`.

---

## 3. MINOR (улучшения качества)

### N1-N17 (от агента, кратко)
- `stripSchemaPrefix` regex может задеть column literal с именем кеша.
- `tryRunInto` глотает exception на debug-уровне — теряем stacktrace в `ErrorRegistry`.
- `MD5` — слабый хеш (для security не годится; для diff — нормально).
- `DayBalancesHasher` round2 + `stringify` scale=6 — двух-ступенчатая нормализация, fragile при изменении `BIGDECIMAL_SCALE`.
- Resolve endpoints не различают «not found» vs «already resolved» — оба возвращают `{updated: 0}`.
- `AdminController.recalc` использует `InitRequestDto` (с openingBalance — silently ignored).
- `Map.of` в fan-out ответах не принимает null → NPE если когда-нибудь `e.toString()` вернёт null.
- `actuator/health` `show-details: always` — leak DB info; в prod надо `when-authorized`.
- PG credentials default `consistency/consistency` в `application.yml` — fail-fast на старте если prod-профиль.
- Inconsistent `ResponseEntity.notFound` — есть в `runById`, нет в других.

### Ignite-side MINOR

**N18. `bumpRecalcDateToOperDay` — потенциальный race с `clearRecalcDate`**
DayBalancesRecalcService — в `dispatchDebouncedRecalc` сначала `bumpRecalcDateToOperDay` (через EntryProcessor — атомарный), потом `recalcRegisterRange`, потом `clearRecalcDate`. Между ними новое событие CQ может снова дёрнуть `bumpRecalcDateToOperDay` → recalc цикл не закончится. Это by-design (correctness > latency), но при шторме событий может крутиться неограниченно. Нужна метрика `recalc_cycles_per_register`.

**N19. `pruneOldType50` deletes даже на 1 type50 запись** (Ignite-side)
Если у регистра только 1 type50 — `MAX(CCOPERATIONDAY) = current.CCOPERATIONDAY` и DELETE отрабатывает с `CCOPERATIONDAY < MAX = ничего не удаляется`. Корректно, но генерирует пустой DELETE на каждом prune — N×DELETE no-op'ов в день. **Фикс:** проверить `COUNT(*) > 1` перед DELETE.

**N20. `findRegistersWithActivityOnDay` сканит и `TURN_DOC_CUR`, и `DAY_BALANCES`**
2 query на тот же день, чтобы найти регистры. Можно UNION'ом в одном запросе → 1 round-trip вместо 2.

**N21. `cleanupAfterStorno` `if (remaining > 0) return;`**
Если 0 — удаляет DayBalances. Но что если `remaining > 0` (есть другие обороты), но **бизнес ожидает что после ВСЕХ сторно DayBalances будет очищен**? Текущее поведение корректно (storno одного оборота из 10 не должно удалять день), но это не задокументировано.

### Документация MINOR

**N22. `REPORT.md` упоминает `INCOME_SALDO` как bootstrap для init**, но в реальном коде initRegister использует `openingBalance` параметр (не INCOME_SALDO). Несоответствие.

**N23. `PLANS.md` упоминает индексы `IDX_TDC_REG_OP`, `IDX_TDC_REG_TYPE_OP`**, но они создаются только в smoke seed. В production `cache-config.xml` имеют ли тот же layout? Нужна сверка.

**N24. Mermaid-диаграммы в REPORT.md не показывают новые компоненты** (Error Registry, EXPLAIN-логирование, affinity probe). Устарели после моих правок.

### Operational MINOR

**N25. `docker-compose.yml` имеет `OPTION_LIBS: ignite-calcite`** хардкод — в production Platform V Datagrid конфигурация будет через `ignite-local.xml`.

**N26. `application.yml` `CONSISTENCY_DEBUG_EXPLAIN: "true"`** включён в smoke — добавляет round-trip на каждый запрос. Нужно `false` по умолчанию в prod profile.

**N27. Нет structured logging**. Logback есть, но нет JSON-формата для интеграции с ELK/Loki.

**N28. Healthcheck PG в docker-compose**, но нет для самого сервиса (`/actuator/health` доступен, но docker compose не использует).

---

## 4. NIT (стиль)

**T1-T15** (от агента + мои):
- Lombok `@Slf4j` непоследовательно (Controllers без logger).
- `DebugSeedController` 800+ строк — split на `DebugSeedController`, `DebugExplainController`, `DebugExecController`.
- Magic numbers: `60_000`, `100` rows, `120` abbreviate, retention defaults — в `ConsistencyProperties`.
- JavaDoc отсутствует на public методах REST controllers.
- Дубликат `explainPrefix` в `DebugSeedController` и `ClusterReader.explainAndLog` — в общий util.
- `Map.of(...)` vs DTO records — выбрать один shape.
- `@Data` на `ConsistencyProperties.Cluster` — достаточно `@Getter @Setter`.
- `LinkedHashMap` где `HashMap` сойдёт.
- `opName.toUpperCase()` без `Locale.ROOT` — turkish "i" bug.
- `BIGDECIMAL_SCALE = 6 HALF_UP` — банковские code-base часто используют `HALF_EVEN`. Уточнить со спекой.
- `@SneakyThrows` на методе без checked exceptions — misleading, убрать.

### Ignite-side NIT

**T16.** `DayBalancesRecalcService` 1500+ строк — кандидат на разбиение на: `DayBalancesRecalcService`, `DayBalancesInitService`, `DayBalancesCleanupService`, `DayBalancesType50Manager`.

**T17.** Закомментированные SQL-альтернативы (`H2 TOP 1` vs `LIMIT 1`) → убрать или вынести в `// История изменений`.

**T18.** `INIT_BATCH_DAYS = 30` — magic number в init; вынести в properties или объяснить в комментарии.

---

## 5. Что хорошо (Strengths)

1. **JDBC repo abstraction** (4 interface + 4 jdbc impl) — swap-ready под DataSpace. Хорошее разделение.
2. **Error Registry** — единая точка ошибок с фильтрами/stats/resolve. Infallible-design (почти, см. M7).
3. **Forward-migration init-anchor в daily cleanup** — нетривиальный кейс решён корректно.
4. **Детерминированный ccIdEKS для type50** — фиксит коренную причину расхождений между кластерами.
5. **Calcite engine с auto-detect EXPLAIN PLAN FOR** в `explainPrefix` — отличный helper.
6. **Affinity probe endpoint** — диагностически очень полезный для production.
7. **`/api/debug/explain-all` + `/analyze-all`** — снимок состояния планов одним вызовом.
8. **9 named-сценариев расхождений в `DebugSeedController`** — coverage для типов багов.
9. **`@AffinityKeyMapped String register`** на keys (TurnDocCurAffinityKey, DayBalancesAffinityKey) — true co-location для JOIN'ов в prod.
10. **`verifyCollocation()` на старте Ignite-кластера** — fail-fast на нарушении инвариантов.
11. **Debounce 1.5с в CQ на TURN_DOC_CUR** — защита от шторма событий.
12. **EXPLAIN-логирование с auto-prefix detection** для H2/Calcite — diagnostic out of the box.
13. **Mermaid-диаграммы** в REPORT.md — наглядность для онбординга.
14. **Init с батчингом по 30 дней** + защита через EXPROP5='init' — memory-bounded для 180-day init.

---

## 6. ТОП-5 действий на ближайший спринт

1. **C1**: вынести все `/api/debug/*` под `@Profile("!prod")`. Это блокер для деплоя.
2. **C4**: `future.join()` → `future.get(timeout, TimeUnit)` с записью ошибки в Error Registry.
3. **M3 + M1**: батчинг `JdbcHashRepository.saveBatch` (chunks по 5k) + chunking `ClusterReader.computeHashes` по дням для TURN_DOC_CUR.
4. **M6**: `ConcurrentHashMap` в `IgniteClientFactory.clients`.
5. **M16**: overlap protection для `scheduledRun()` через `AtomicBoolean isRunning`.

После этих 5 правок система готова к staging-окружению. Для production нужны ещё M2 (cap на mismatch count), M8 (transactions), M15 (error retention) + добавить аутентификацию.

---

## 7. Что отсутствует (для full production readiness)

| Категория | Отсутствует |
|---|---|
| **Auth** | Spring Security / JWT / OAuth2 — все endpoints открыты |
| **Tests** | Ни одного unit/integration теста |
| **CI/CD** | Нет Jenkinsfile / GitHub Actions для consistency-service |
| **Метрики** | Actuator/Prometheus подключён, но нет custom metrics (run duration, mismatch count, hash latency per cluster/cache) |
| **Алертинг** | Нет webhook-интеграции (Slack/Telegram) — упомянуто в Roadmap |
| **DataSpace impl** | Только интерфейсы + JDBC impl. DataSpace-репозитории — отдельный спринт |
| **EnrichDirectory hasher** | Пропущен по спеке (нужен `CCVERSION`/`RQUID` в DTO) |
| **Multi-cluster verification на multi-node Ignite** | Smoke single-node; affinity co-location проверяется только в spec/cache-config.xml |
| **Backup стратегия** | Нет — PG может потеряться, потребуется replay через ANALYZE/scheduled run |

---

## Файлы для применения правок (в порядке приоритета)

1. `consistency-service/src/main/java/.../rpc/DebugSeedController.java` — C1, C2, T2 (split)
2. `consistency-service/src/main/java/.../lib/ConsistencyJob.java` — C4, M2, M8, M10, M16
3. `consistency-service/src/main/java/.../lib/ClusterReader.java` — M1, C5
4. `consistency-service/src/main/java/.../integration/ignite/IgniteClientFactory.java` — C3, M6
5. `consistency-service/src/main/java/.../lib/repo/jdbc/JdbcHashRepository.java` — M3
6. `consistency-service/src/main/java/.../lib/repo/jdbc/JdbcMismatchRepository.java` — M4
7. `stmnt-ignite_precalc/.../utility/DayBalancesRecalcService.java` — C6, M11, N19, N20

Ничего не правлю сейчас — это ревью на согласование. Жду приоритезации.
