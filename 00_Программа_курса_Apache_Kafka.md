# Программа курса Apache Kafka

## 1. Общие параметры

Продолжительность: 40 академических часов.

Техническая платформа:

- Rocky Linux 9.
- Java 17.
- Apache Kafka в режиме KRaft.
- Python 3.11 или новее.
- PostgreSQL 16 для внешних подключений.
- Prometheus и Grafana для мониторинга.

Основной формат практической работы:

- запуск Kafka broker и Kafka cluster;
- настройка topics, partitions, replication, retention и security;
- подключение Python producer и Python consumer;
- интеграция Kafka с PostgreSQL через Python;
- проверка отказоустойчивости, consumer lag, offsets и состояния ISR;
- анализ конфигураций и ожидаемого вывода команд.

## 2. Состав модулей

| N | Раздел | Часы | Основные темы | Практический результат |
|---|--------|------|---------------|------------------------|
| 1 | Базовая архитектура Apache Kafka | 2 | Event streaming, broker, topic, partition, offset, producer, consumer, consumer group | Понимание цепочки `producer -> Kafka -> consumer` и базовых терминов |
| 2 | Standalone broker в KRaft mode | 4 | Java, установка Kafka, KRaft storage, combined broker/controller, listeners | Запущен один broker, создан topic, выполнены запись и чтение |
| 3 | Topics, partitions, keys и offsets | 4 | Partitions, key-based routing, offsets, ordering, consumer groups | Созданы topics с несколькими partitions, проверено распределение records |
| 4 | Producers и consumers на Python | 6 | Producer API, delivery callback, Consumer API, manual commit, at-least-once | Реализованы Python producer и consumer с ручной фиксацией offset |
| 5 | Multi-broker cluster | 6 | Cluster, replication factor, leader/follower, ISR, leader election, отказ broker | Развернут cluster из трех brokers, проверена репликация и отказоустойчивость |
| 6 | Storage, retention и compaction | 4 | Log segments, retention by time/size, cleanup policy, compaction, page cache | Настроены retention и compaction, проанализированы segment files |
| 7 | Внешние подключения через Python и Kafka Connect | 4 | PostgreSQL source/sink, идемпотентная запись, Kafka Connect worker | Реализована интеграция Kafka и PostgreSQL через Python |
| 8 | Stream processing через Python | 3 | Input topic, processing loop, aggregation, state store, output topic | Реализован потоковый обработчик с записью результата в отдельный topic |
| 9 | TLS, ACL и безопасность Kafka | 3 | TLS listeners, certificates, principals, ACL, authorization | Настроены защищенный listener и права доступа для producer/consumer |
| 10 | Monitoring и итоговый pipeline | 4 | JMX exporter, Prometheus, Grafana, consumer lag, финальный event-driven сценарий | Собран pipeline обработки заказов и выполнена диагностика состояния |

## 3. Раздел 1. Базовая архитектура Apache Kafka

Теоретические темы:

- Apache Kafka как распределенный журнал событий.
- Event, record, key, value, headers и timestamp.
- Broker как сервер Kafka, который принимает, хранит и отдает records.
- Cluster как набор brokers.
- Topic как именованный поток событий.
- Partition как физический упорядоченный журнал внутри topic.
- Offset как позиция record внутри partition.
- Producer как приложение, записывающее события.
- Consumer как приложение, читающее события.
- Consumer group как набор consumers с общим `group.id`.
- Replica, leader, follower и ISR.

Практический результат:

- описана базовая схема движения события;
- определены координаты record: topic, partition, offset;
- разобрано различие между Kafka и point-to-point message broker.

## 4. Раздел 2. Standalone broker в KRaft mode

Теоретические темы:

- KRaft mode без ZooKeeper.
- Роли `broker` и `controller`.
- `node.id`, `process.roles`, `controller.quorum.voters`.
- `listeners` и `advertised.listeners`.
- `log.dirs` и хранение данных broker.
- Форматирование storage через `kafka-storage.sh format`.

Практический результат:

- установлены Java и Kafka;
- создан системный пользователь `kafka`;
- подготовлены `/opt/kafka`, `/var/lib/kafka`, `/var/log/kafka`;
- отформатирован KRaft storage;
- запущен standalone broker;
- создан topic `test-events`;
- выполнены запись и чтение records через CLI.

## 5. Раздел 3. Topics, partitions, keys и offsets

Теоретические темы:

- Topic как логический поток.
- Partition как единица параллелизма и порядка.
- Key-based partitioning.
- Offset как неизменяемая позиция record.
- Consumer group assignment.
- Разница между чтением одной group и разными groups.
- Влияние увеличения partitions на порядок и маршрутизацию key.

Практический результат:

- создан topic с несколькими partitions;
- отправлены records с keys;
- проверено распределение records по partitions;
- просмотрены offsets и consumer group state;
- выполнен сброс offset для consumer group.

## 6. Раздел 4. Producers и consumers на Python

Теоретические темы:

- Kafka producer lifecycle.
- Сериализация JSON.
- `bootstrap.servers`, `acks`, `retries`, `linger.ms`, `batch.size`.
- Delivery callback и подтверждение записи.
- Kafka consumer polling loop.
- `group.id`, `enable.auto.commit`, `auto.offset.reset`.
- Manual commit.
- At-most-once, at-least-once и exactly-once на уровне прикладной архитектуры.

Практический результат:

- создан Python producer для JSON records;
- создан Python consumer с ручным commit;
- проверена обработка ошибок доставки;
- проверена балансировка consumer group;
- зафиксирован порядок `poll -> process -> commit`.

## 7. Раздел 5. Multi-broker cluster

Теоретические темы:

- Kafka cluster из нескольких brokers.
- Replication factor.
- Leader и follower replicas.
- ISR и `min.insync.replicas`.
- `acks=all` и гарантия подтверждения записи.
- Leader election при отказе broker.
- Влияние `unclean.leader.election.enable`.

Практический результат:

- развернут cluster из трех brokers;
- настроены отдельные `node.id`, listeners и data dirs;
- создан topic с replication factor 3;
- проверены leader и replicas;
- остановлен один broker;
- проверено продолжение записи и чтения при отказе.

## 8. Раздел 6. Storage, retention и compaction

Теоретические темы:

- Append-only log.
- Segment files: `.log`, `.index`, `.timeindex`.
- Retention by time и retention by size.
- Topic-level и broker-level параметры хранения.
- Cleanup policy `delete`.
- Cleanup policy `compact`.
- Влияние partitions и replicas на объем диска.

Практический результат:

- найдены segment files на диске;
- настроен retention для topic;
- проверено удаление старых segments;
- настроен compacted topic;
- проверено сохранение последнего значения по key.

## 9. Раздел 7. Внешние подключения через Python и Kafka Connect

Теоретические темы:

- Kafka как слой между приложением и внешним состоянием.
- PostgreSQL source pattern.
- PostgreSQL sink pattern.
- Идемпотентная запись во внешнюю базу.
- Согласование Kafka offset и состояния PostgreSQL.
- Kafka Connect worker.
- Source connector и sink connector.

Практический результат:

- создана таблица PostgreSQL;
- написан Python source, публикующий события из PostgreSQL в Kafka;
- написан Python sink, читающий Kafka topic и записывающий в PostgreSQL;
- проверена повторная обработка без дублей через idempotency key;
- подготовлен базовый Kafka Connect worker config.

## 10. Раздел 8. Stream processing через Python

Теоретические темы:

- Потоковая обработка records.
- Stateless и stateful processing.
- Aggregation by key.
- State store во внешней базе или памяти процесса.
- Changelog topic как журнал восстановления состояния.
- Output topic как результат обработки.

Практический результат:

- создан input topic;
- создан Python processor;
- реализована агрегация по key;
- результат записан в output topic;
- проверено восстановление состояния после перезапуска.

## 11. Раздел 9. TLS, ACL и безопасность Kafka

Теоретические темы:

- Plaintext listener и SSL listener.
- TLS handshake.
- Keystore и truststore.
- Principal identity.
- ACL resources: Topic, Group, Cluster.
- ACL operations: Read, Write, Create, Describe, Alter.
- Разделение transport security и authorization.

Практический результат:

- создан certificate authority;
- выпущен broker certificate;
- настроен SSL listener;
- настроен Python client для TLS;
- включен authorizer;
- созданы ACL для producer и consumer;
- проверен отказ доступа без нужных прав.

## 12. Раздел 10. Monitoring и итоговый pipeline

Теоретические темы:

- JMX metrics Kafka.
- JMX exporter.
- Prometheus scrape config.
- Grafana dashboard.
- Consumer lag.
- Under-replicated partitions.
- Offline partitions.
- Диагностика producer, broker, consumer и storage.

Практический результат:

- подключен JMX exporter;
- настроен Prometheus;
- добавлена Grafana dashboard;
- проверены broker metrics;
- проверен consumer lag;
- собран pipeline обработки заказов:

```text
HTTP API -> orders topic -> billing consumer -> PostgreSQL
                     |
                     +-> shipping consumer -> PostgreSQL
                     |
                     +-> audit consumer -> audit topic
```

## 13. Итоговое состояние стенда

После выполнения всех разделов в рабочей среде присутствуют:

- Kafka standalone broker.
- Kafka cluster из трех brokers.
- Topics с разными partitions, replication factor и cleanup policies.
- Python producer и Python consumer.
- Python HTTP API, публикующий события в Kafka.
- Python processors для потоковой обработки.
- PostgreSQL source/sink интеграции.
- TLS listener и ACL rules.
- Prometheus и Grafana для мониторинга Kafka.
- Практические команды для проверки broker, topics, offsets, consumer groups, ISR, lag и storage.

