# Smoke ignite-узел с встроенным CDC publisher.
#
# Базируется на public apacheignite/ignite:2.16.0. Сверху подкладываем
# uber-jar нашего CDC-publisher'а в libs/ — Ignite автоматически грузит
# все JAR'ы из libs/. ignite-config-template.xml ссылается на bean
# ru.sbrf.stmnt.ignite.kafka.cdc.KafkaCdcPublisherBean — он стартует на
# AFTER_NODE_START, шлёт CDC-события в Kafka через ContinuousQuery.
#
# Cluster-id передаём через JVM_OPTS (см. docker-compose).
FROM apacheignite/ignite:2.16.0

# Подкладываем CDC uber-jar (содержит наши классы + kafka-clients + jackson).
COPY stmnt-consistency-ignite-cdc/target/stmnt-consistency-ignite-cdc-1.0-SNAPSHOT-uber.jar \
     /opt/ignite/apache-ignite/libs/cdc-publisher.jar

# Hashers ссылаются на CdcEvent/CdcEventHasher из shared-модуля; uber-jar их уже несёт.
# Calcite-libs нужны (мы оставляем OPTION_LIBS=ignite-calcite в compose).
