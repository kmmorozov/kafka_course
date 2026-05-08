# Production Readiness Apache Kafka

Файл фиксирует технические проверки перед эксплуатацией Kafka-стенда или переносом конфигурации в production-like среду.

Структура проверки:

```text
architecture -> quorum -> replication -> storage -> clients -> security -> monitoring -> operations
```

## 1. Минимальная production-like архитектура

Standalone broker подходит для проверки команд, clients и базовой логики. Для отказоустойчивой среды нужен cluster.

Минимальная схема:

```text
              +---------------------+
              | controller quorum   |
              | 3 voters            |
              +----------+----------+
                         |
        +----------------+----------------+
        |                |                |
        v                v                v
   +---------+      +---------+      +---------+
   | broker1 |      | broker2 |      | broker3 |
   +----+----+      +----+----+      +----+----+
        |                |                |
        +-------- replicated topics ------+
```

Технические требования:

- нечетное количество controllers для quorum: 3, 5, 7;
- минимум 3 brokers для `replication.factor=3`;
- отдельные data dirs для brokers;
- одинаковый `cluster.id` у всех nodes;
- уникальный `node.id` у каждого node;
- `advertised.listeners` должны содержать адреса, доступные clients;
- internal traffic и client traffic должны быть разделены отдельными listeners, если среда не лабораторная.

## 2. KRaft quorum

KRaft quorum хранит metadata Kafka: topics, partitions, ACL, cluster state. Если quorum недоступен, Kafka может продолжать обслуживать часть уже известных операций, но metadata changes будут недоступны или нестабильны.

Проверка:

```bash
/opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  describe \
  --status
```

Ожидаемый вывод для quorum из трех voters:

```text
LeaderId: 1
CurrentVoters: [1,2,3]
CurrentObservers: []
```

Технические правила:

- для 3 voters должен быть доступен минимум 2 voters;
- для 5 voters должен быть доступен минимум 3 voters;
- потеря большинства controllers блокирует изменения metadata;
- `controller.quorum.voters` должен быть одинаковым на всех nodes;
- controller listener должен быть доступен другим controllers.

Проверка config:

```bash
grep -E '^(node.id|process.roles|controller.quorum.voters|controller.listener.names|listeners)=' \
  /opt/kafka/config/kraft/server.properties
```

## 3. Replication и durability

Replication защищает partition от потери broker. Producer получает подтверждение записи только после выполнения условий `acks` и `min.insync.replicas`.

Базовая production-like комбинация:

```properties
default.replication.factor=3
min.insync.replicas=2
unclean.leader.election.enable=false
```

Producer config:

```properties
acks=all
enable.idempotence=true
retries=2147483647
```

Влияние параметров:

- `default.replication.factor=3` создает три copies partition по умолчанию.
- `min.insync.replicas=2` требует минимум две актуальные replicas для успешной записи при `acks=all`.
- `unclean.leader.election.enable=false` запрещает выбирать leader из неактуальной replica и снижает риск потери подтвержденных records.
- `acks=all` требует подтверждения leader после записи в достаточное число ISR.
- `enable.idempotence=true` защищает producer от дублей при retry на уровне producer session.

Проверка topic:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders
```

Ожидаемый production-like вывод:

```text
Topic: orders PartitionCount: 6 ReplicationFactor: 3
Partition: 0 Leader: 1 Replicas: 1,2,3 Isr: 1,2,3
```

Плохие признаки:

```text
ReplicationFactor: 1
Leader: -1
Isr меньше min.insync.replicas
Replicas содержит broker, который постоянно отсутствует в Isr
```

## 4. Расчет partitions

Partition определяет параллелизм записи, чтения и хранения. Увеличить partitions можно, уменьшить штатно нельзя.

Основные ограничения:

- порядок records гарантирован только внутри одной partition;
- consumer group не может активно использовать consumers больше, чем partitions topic;
- большое число partitions увеличивает metadata, file handles, recovery time и нагрузку на controller;
- изменение числа partitions меняет key distribution для default partitioner.

Практическая формула выбора:

```text
partitions = max(
  required_consumer_parallelism,
  ceil(target_topic_throughput / safe_partition_throughput),
  required_key_spread
)
```

Параметры формулы:

- `required_consumer_parallelism` - максимальное число consumers в одной group, которые должны работать параллельно.
- `target_topic_throughput` - целевой throughput topic.
- `safe_partition_throughput` - throughput одной partition с запасом, измеренный нагрузочным тестом.
- `required_key_spread` - число независимых key-групп, которые нужно распределить по partitions.

Проверка текущего числа partitions:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders
```

Увеличение partitions:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic orders \
  --partitions 12
```

Влияние:

```text
Новые records могут начать попадать в другие partitions.
Для key-ordered потоков увеличение partitions требует проверки бизнес-инвариантов порядка.
```

## 5. Расчет диска и retention

Kafka хранит records на диске до удаления retention или compaction. Replication умножает физический объем.

Базовая оценка:

```text
physical_storage =
  stored_bytes_per_day
  * retention_days
  * replication_factor
  * overhead_factor
```

Где:

- `stored_bytes_per_day` - объем после producer compression и Kafka storage overhead, лучше брать из метрик broker.
- `retention_days` - срок хранения.
- `replication_factor` - число copies.
- `overhead_factor` - запас на indexes, active segments, compaction, skew и операционный резерв; обычно не меньше 1.2.

Пример:

```text
stored_bytes_per_day = 500 GB
retention_days = 7
replication_factor = 3
overhead_factor = 1.3

physical_storage = 500 * 7 * 3 * 1.3 = 13650 GB
```

Проверка фактического использования:

```bash
df -h /var/lib/kafka
sudo du -h --max-depth=2 /var/lib/kafka | sort -h | tail -20
```

Технические пороги:

- выше 70% disk usage нужно проверять retention и рост topics;
- выше 80% нужен план освобождения места или расширения диска;
- выше 90% broker может начать падать или терять способность писать segments.

Настройка topic retention:

```bash
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name orders \
  --alter \
  --add-config retention.ms=604800000,retention.bytes=107374182400
```

Влияние:

```text
retention.ms ограничивает возраст records.
retention.bytes ограничивает размер на partition, а не на весь topic.
Topic с 10 partitions и retention.bytes=100GB может занять до 1000GB до учета replicas.
```

## 6. Producer readiness

Producer должен явно задавать режим подтверждения, retries, idempotence, serialization и timeout.

Базовый reliable producer config:

```properties
bootstrap.servers=broker1:9092,broker2:9092,broker3:9092
acks=all
enable.idempotence=true
retries=2147483647
delivery.timeout.ms=120000
request.timeout.ms=30000
linger.ms=5
batch.size=65536
compression.type=zstd
```

Параметры и влияние:

- `bootstrap.servers` должен содержать несколько brokers, чтобы client мог получить metadata при отказе одного broker.
- `acks=all` повышает durability, но зависит от ISR и `min.insync.replicas`.
- `enable.idempotence=true` снижает риск дублей при retries.
- `retries` должен быть включен для временных сетевых ошибок.
- `delivery.timeout.ms` ограничивает полный срок доставки record.
- `request.timeout.ms` ограничивает ожидание ответа broker на отдельный request.
- `linger.ms` добавляет небольшое ожидание для batch и повышает throughput.
- `batch.size` задает размер batch на partition.
- `compression.type=zstd` уменьшает сетевой и дисковый объем, но добавляет CPU cost.

Проверка delivery callback в Python:

```python
def delivery_report(err, msg):
    if err is not None:
        raise RuntimeError(f"delivery failed: {err}")
    print(f"delivered topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")
```

Ожидаемый вывод:

```text
delivered topic=orders partition=2 offset=1042
```

## 7. Consumer readiness

Consumer должен фиксировать offset только после успешной обработки record.

Базовый reliable consumer config:

```properties
bootstrap.servers=broker1:9092,broker2:9092,broker3:9092
group.id=orders-billing
enable.auto.commit=false
auto.offset.reset=earliest
max.poll.records=100
max.poll.interval.ms=300000
session.timeout.ms=45000
heartbeat.interval.ms=15000
isolation.level=read_committed
```

Параметры и влияние:

- `group.id` определяет consumer group и ее offsets.
- `enable.auto.commit=false` переносит commit под контроль приложения.
- `auto.offset.reset=earliest` применяется только если committed offset отсутствует.
- `max.poll.records` ограничивает batch records за один `poll`.
- `max.poll.interval.ms` должен быть больше максимального времени обработки batch.
- `session.timeout.ms` задает срок, после которого broker считает consumer недоступным.
- `heartbeat.interval.ms` должен быть меньше `session.timeout.ms`.
- `isolation.level=read_committed` скрывает незавершенные transactional records.

Правильная последовательность:

```text
poll -> validate -> process -> write external state -> commit offset
```

Проверка lag:

```bash
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server broker1:9092 \
  --describe \
  --group orders-billing
```

Ожидаемый нормальный признак:

```text
LAG стабилен или стремится к 0 при штатной нагрузке
```

## 8. Security readiness

Минимальная production-like безопасность:

- client traffic через TLS;
- отдельные principals для producer, consumer, admin и Connect;
- ACL по принципу минимальных прав;
- запрет auto topic creation;
- секреты вне Git;
- отдельные listeners для internal replication и external clients.

Broker settings:

```properties
auto.create.topics.enable=false
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
allow.everyone.if.no.acl.found=false
super.users=User:kafka-admin
```

Влияние:

- `auto.create.topics.enable=false` запрещает случайное создание topics из-за опечатки client.
- `authorizer.class.name` включает ACL authorization.
- `allow.everyone.if.no.acl.found=false` запрещает доступ, если ACL отсутствуют.
- `super.users` задает principals, которые имеют административный доступ.

Проверка ACL:

```bash
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server broker1:9092 \
  --list
```

Минимальные ACL для producer:

```bash
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server broker1:9092 \
  --add \
  --allow-principal User:orders-api \
  --operation Write \
  --operation Describe \
  --topic orders
```

Минимальные ACL для consumer:

```bash
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server broker1:9092 \
  --add \
  --allow-principal User:billing-service \
  --operation Read \
  --operation Describe \
  --topic orders

/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server broker1:9092 \
  --add \
  --allow-principal User:billing-service \
  --operation Read \
  --group orders-billing
```

## 9. OS и JVM readiness

Kafka интенсивно использует disk I/O, network I/O, page cache и file descriptors.

Проверка limits:

```bash
sudo systemctl show kafka | grep -E 'LimitNOFILE|LimitNPROC'
```

Ожидаемые значения:

```text
LimitNOFILE=100000
```

Проверка heap:

```bash
sudo systemctl cat kafka | grep KAFKA_HEAP_OPTS
```

Базовый вариант для небольшого стенда:

```text
KAFKA_HEAP_OPTS="-Xms2G -Xmx2G"
```

Технические правила:

- `-Xms` и `-Xmx` обычно задаются одинаковыми, чтобы избежать resize heap;
- слишком большой heap уменьшает page cache;
- слишком маленький heap ведет к частым GC;
- Kafka хранит data через page cache, поэтому свободная RAM важна для throughput.

Проверка page cache и memory:

```bash
free -h
vmstat 1 5
```

## 10. Monitoring readiness

Минимальный набор метрик:

- broker alive;
- offline partitions;
- under-replicated partitions;
- active controller count;
- request latency;
- bytes in/out;
- produce/fetch request rate;
- consumer lag;
- disk usage;
- JVM heap;
- GC pause;
- open file descriptors.

Проверка JMX exporter:

```bash
curl -sS http://localhost:7071/metrics | head
```

Ожидаемый вывод:

```text
# HELP kafka_...
# TYPE kafka_...
```

Проверка Prometheus target:

```bash
curl -sS http://localhost:9090/api/v1/targets
```

Критичные alert conditions:

```text
UnderReplicatedPartitions > 0
OfflinePartitionsCount > 0
ActiveControllerCount != 1
ConsumerLag grows continuously
DiskUsedPercent > 80
Broker process down
JMX exporter target down
```

## 11. Topic design checklist

Перед созданием topic фиксируются:

```text
topic.name
event schema
key field
partitions
replication.factor
min.insync.replicas
cleanup.policy
retention.ms
retention.bytes
compression expectations
owner application
consumer groups
security principals
```

Шаблон:

```text
topic.name: orders
event schema: OrderCreated v1
key field: order_id
partitions: 12
replication.factor: 3
min.insync.replicas: 2
cleanup.policy: delete
retention.ms: 604800000
retention.bytes: 107374182400
owner application: orders-api
consumer groups: billing-service, shipping-service, audit-service
producer principal: User:orders-api
consumer principals: User:billing-service, User:shipping-service, User:audit-service
```

Создание:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker1:9092 \
  --create \
  --topic orders \
  --partitions 12 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config retention.bytes=107374182400
```

## 12. Backup и восстановление

Kafka не является заменой backup внешних систем. Kafka хранит event log в пределах retention. Если retention удалил record, Kafka не восстановит его без внешнего архива.

Что сохранять:

- broker configs;
- systemd unit files;
- TLS CA, certificates и renewal procedure;
- ACL export;
- topic specifications;
- Prometheus/Grafana configs;
- Python application configs без секретов;
- schema definitions, если используется schema registry.

Экспорт ACL:

```bash
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server broker1:9092 \
  --list > kafka-acls-export.txt
```

Экспорт topic descriptions:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker1:9092 \
  --describe > kafka-topics-describe.txt
```

Техническое ограничение:

```text
Файловое копирование log.dirs живого broker не является корректным backup Kafka topic.
Для переноса данных используются MirrorMaker 2, cluster replication, application replay или внешнее архивирование событий.
```

## 13. Change readiness

Перед изменением Kafka config фиксируются:

- текущие значения параметров;
- affected brokers;
- affected topics;
- expected client impact;
- rollback action;
- команды проверки после изменения.

Пример изменения topic retention:

```bash
# Текущее значение.
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server broker1:9092 \
  --entity-type topics \
  --entity-name orders \
  --describe

# Изменение.
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server broker1:9092 \
  --entity-type topics \
  --entity-name orders \
  --alter \
  --add-config retention.ms=1209600000

# Проверка.
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server broker1:9092 \
  --entity-type topics \
  --entity-name orders \
  --describe
```

Rollback:

```bash
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server broker1:9092 \
  --entity-type topics \
  --entity-name orders \
  --alter \
  --add-config retention.ms=604800000
```

## 14. Итоговый readiness checklist

```text
[ ] Controllers имеют quorum.
[ ] Все brokers имеют одинаковый cluster.id.
[ ] Каждый broker имеет уникальный node.id.
[ ] advertised.listeners доступны clients.
[ ] Replication factor production topics равен 3 или больше.
[ ] min.insync.replicas согласован с replication factor.
[ ] Producers используют acks=all.
[ ] Producers используют enable.idempotence=true.
[ ] Consumers используют manual commit для внешнего состояния.
[ ] Consumer lag мониторится.
[ ] Retention настроен явно для critical topics.
[ ] Disk usage ниже 70% в штатном режиме.
[ ] auto.create.topics.enable=false.
[ ] TLS включен для client traffic.
[ ] ACL включены и проверены.
[ ] Secrets не хранятся в Git.
[ ] JMX exporter доступен.
[ ] Prometheus targets active.
[ ] Alerts настроены для ISR, offline partitions, lag и disk.
[ ] Topic specifications сохранены.
[ ] ACL export сохранен.
[ ] Runbook диагностики доступен.
```

