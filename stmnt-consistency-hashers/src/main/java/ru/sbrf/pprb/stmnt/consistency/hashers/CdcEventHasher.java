package ru.sbrf.pprb.stmnt.consistency.hashers;

/**
 * SHARED. Контракт хешера, который Ignite-сторона дёргает в ContinuousQuery
 * callback'е на одну запись кеша.
 *
 * Реализации лежат на стороне Ignite (там, где доступны DTO-классы — они
 * не публикуются как общие зависимости, поэтому интерфейс параметризован
 * Object/Object и реализация кастит сама).
 *
 * Возвращает businessKey и hash; clusterId/op/ts добавляет publisher.
 */
public interface CdcEventHasher {

    /** Логическое имя кеша (REGISTER, TURN_DOC_CUR, …). */
    String cacheName();

    /**
     * Извлечь бизнес-ключ из cache entry (для cross-cluster join'а).
     * Чаще всего objectId. Для DAY_BALANCES — register:operationDay.
     */
    String businessKeyOf(Object key, Object value);

    /**
     * Канонический hash значения. Реализация должна использовать HashUtil.md5Of
     * с теми же полями и тем же порядком, что у соответствующего pull-mode
     * хешера в consistency-service.
     */
    String hashOf(Object key, Object value);
}
