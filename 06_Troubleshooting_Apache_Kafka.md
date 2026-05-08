# Troubleshooting Apache Kafka

Файл описывает диагностику Kafka-стенда по схеме:

```text
symptom -> check command -> expected output -> technical cause -> fix
```

Команды рассчитаны на Rocky Linux 9, Kafka в KRaft mode, каталог Kafka `/opt/kafka`, данные `/var/lib/kafka`, Python-приложения в `/home/student/kafka-course-python`.

## 1. Базовая цепочка диагностики

Проверка Kafka выполняется по слоям. Если нижний слой не работает, верхний слой проверять бессмысленно.

```text
process -> TCP listener -> Kafka protocol -> metadata -> topic -> producer -> consumer -> external state
```

### 1.1. Процесс Kafka

```bash
# Проверка состояния systemd unit.
sudo systemctl status kafka
```

Ожидаемый вывод:

```text
Active: active (running)
```

Если вывод содержит `failed`, `activating` или процесс постоянно перезапускается, broker не дошел до рабочего состояния.

### 1.2. TCP listener

```bash
# Проверка, что broker слушает client port.
ss -ltnp | grep 9092
```

Ожидаемый вывод:

```text
LISTEN ... 127.0.0.1:9092 ...
```

Если строки нет, Kafka не открыла listener. Причина обычно в ошибке `listeners`, занятом порте, ошибке KRaft storage или падении JVM.

### 1.3. Kafka protocol

```bash
# Проверка ответа broker на Kafka API.
/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

Ожидаемый вывод:

```text
localhost:9092 (id: 1 rack: null) -> (
  Produce(...)
  Fetch(...)
  Metadata(...)
)
```

Если TCP port открыт, но команда зависает или падает, listener может быть настроен не на тот security protocol, client подключается не к тому listener, либо broker не завершил инициализацию.

## 2. Broker не стартует

### Симптом

```text
systemctl status kafka
Active: failed
```

### Проверка логов

```bash
# Последние сообщения systemd unit.
sudo journalctl -u kafka -n 100 --no-pager
```

Ожидаемый полезный фрагмент при успешном старте:

```text
Kafka Server started
```

### Причина 1. Не отформатирован KRaft storage

Типичный вывод:

```text
No readable meta.properties files found
```

Проверка:

```bash
sudo ls -la /var/lib/kafka/kraft-combined-logs/meta.properties
```

Ожидаемый вывод:

```text
/var/lib/kafka/kraft-combined-logs/meta.properties
```

Исправление:

```bash
# Создать cluster id.
KAFKA_CLUSTER_ID=$(/opt/kafka/bin/kafka-storage.sh random-uuid)

# Отформатировать storage один раз перед первым запуском broker.
sudo /opt/kafka/bin/kafka-storage.sh format \
  -t "$KAFKA_CLUSTER_ID" \
  -c /opt/kafka/config/kraft/server.properties
```

Влияние команды:

```text
kafka-storage.sh format создает meta.properties и metadata log.
Повторное форматирование рабочего storage создает новый cluster.id и делает старые данные несовместимыми с broker.
```

### Причина 2. Несовпадение `node.id` и `controller.quorum.voters`

Типичный вывод:

```text
If using process.roles, node.id must be set
```

или:

```text
Controller quorum voters must contain the node id
```

Проверка:

```bash
grep -E '^(node.id|process.roles|controller.quorum.voters)=' /opt/kafka/config/kraft/server.properties
```

Ожидаемый standalone-вариант:

```properties
node.id=1
process.roles=broker,controller
controller.quorum.voters=1@localhost:9093
```

Исправление:

```text
node.id должен совпадать с id в controller.quorum.voters.
Если process.roles содержит controller, этот node должен быть указан в controller.quorum.voters.
Если process.roles содержит только broker, node не должен быть controller voter.
```

### Причина 3. Listener использует занятый port

Типичный вывод:

```text
Address already in use
```

Проверка:

```bash
ss -ltnp | grep -E ':9092|:9093'
```

Ожидаемый вывод до старта Kafka:

```text
нет строк с занятыми портами 9092 и 9093
```

Исправление:

```text
Остановить процесс, который занимает port, или изменить listeners в server.properties.
Для multi-broker cluster каждый broker на одном host должен иметь уникальные ports.
```

### Причина 4. Нет прав на каталог данных

Типичный вывод:

```text
Permission denied
```

Проверка:

```bash
sudo ls -ld /var/lib/kafka /var/lib/kafka/kraft-combined-logs
```

Ожидаемый вывод:

```text
drwxr-xr-x kafka kafka ... /var/lib/kafka
drwxr-xr-x kafka kafka ... /var/lib/kafka/kraft-combined-logs
```

Исправление:

```bash
sudo chown -R kafka:kafka /var/lib/kafka /var/log/kafka /opt/kafka
```

## 3. Broker стартует, но client не подключается

### Симптом

```text
Connection refused
```

или:

```text
Timed out waiting for a node assignment
```

### Проверки

```bash
ss -ltnp | grep 9092
/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
grep -E '^(listeners|advertised.listeners|listener.security.protocol.map)=' /opt/kafka/config/kraft/server.properties
```

Ожидаемый standalone plaintext config:

```properties
listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
advertised.listeners=PLAINTEXT://localhost:9092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
```

### Причина 1. Неверный `advertised.listeners`

`listeners` задает адрес, на котором broker слушает соединения. `advertised.listeners` задает адрес, который broker возвращает client в metadata response.

Если client подключается к `localhost:9092`, но metadata содержит `broker1:9092`, client после первого запроса начнет подключаться к `broker1:9092`.

Исправление:

```text
Для локального стенда advertised.listeners должен содержать адрес, доступный client-приложению.
Для Docker, VM или удаленного host нельзя без проверки оставлять localhost, потому что localhost внутри client и localhost внутри broker могут быть разными машинами.
```

### Причина 2. Несовпадение security protocol

Типичный вывод:

```text
Disconnected while requesting ApiVersion
```

Проверка:

```bash
grep -E '^(listeners|listener.security.protocol.map|security.inter.broker.protocol)=' /opt/kafka/config/kraft/server.properties
```

Исправление:

```text
Если listener объявлен как SSL, client должен использовать security.protocol=SSL.
Если listener объявлен как PLAINTEXT, client не должен включать SSL-настройки.
```

## 4. Topic не создается

### Симптом

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic test-events \
  --partitions 1 \
  --replication-factor 1
```

Ошибка:

```text
Timed out waiting for a node assignment
```

или:

```text
Replication factor: 3 larger than available brokers: 1
```

### Проверки

```bash
/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
```

Ожидаемый вывод для одного broker:

```text
LeaderId: 1
CurrentVoters: [1]
```

### Причина 1. Replication factor больше числа brokers

Исправление для standalone:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic test-events \
  --partitions 1 \
  --replication-factor 1
```

Исправление для cluster из трех brokers:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 3
```

### Причина 2. Topic уже существует

Типичный вывод:

```text
Topic 'test-events' already exists.
```

Проверка:

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Исправление:

```text
Если topic нужен с другими параметрами, изменяются только поддерживаемые параметры.
Количество partitions можно увеличить, но нельзя уменьшить штатной командой.
Replication factor изменяется через reassignment.
```

## 5. Producer не отправляет records

### Симптом CLI producer

```text
WARN Bootstrap broker localhost:9092 disconnected
```

или:

```text
NOT_ENOUGH_REPLICAS
```

### Проверки

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic test-events
```

Ожидаемый вывод:

```text
Topic: test-events  PartitionCount: 1  ReplicationFactor: 1
Topic: test-events  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
```

### Причина 1. Нет leader у partition

Плохой признак:

```text
Leader: none
```

или:

```text
Leader: -1
```

Исправление:

```text
Проверить, жив ли broker с leader replica.
Проверить ISR.
Проверить controller quorum.
Для cluster запустить недостающий broker или выполнить partition reassignment.
```

### Причина 2. `acks=all` и недостаточно ISR

Типичный вывод:

```text
NOT_ENOUGH_REPLICAS
```

Проверка topic config:

```bash
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name orders \
  --describe
```

Проверка broker config:

```bash
grep -E '^(min.insync.replicas|default.replication.factor)=' /opt/kafka/config/kraft/server.properties
```

Исправление:

```text
Вернуть brokers в ISR или снизить min.insync.replicas для стенда.
Для production снижение min.insync.replicas уменьшает гарантию сохранности записи.
```

## 6. Python producer не работает

### Симптом

```text
KafkaException: KafkaError{code=_TRANSPORT,val=-195,str="Failed to resolve ..."}
```

или:

```text
Local: Message timed out
```

### Проверка `.env`

```bash
cd /home/student/kafka-course-python
grep -E '^(KAFKA_BOOTSTRAP_SERVERS|KAFKA_SECURITY_PROTOCOL)=' .env
```

Ожидаемый plaintext-вариант:

```dotenv
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
```

### Проверка Python-зависимостей

```bash
cd /home/student/kafka-course-python
source .venv/bin/activate
python -c "import confluent_kafka; print(confluent_kafka.version())"
```

Ожидаемый вывод:

```text
('...', ...)
```

### Причина 1. Неверный bootstrap server

Исправление:

```text
KAFKA_BOOTSTRAP_SERVERS должен содержать хотя бы один доступный broker.
Для cluster лучше указывать несколько brokers через запятую: host1:9092,host2:9092,host3:9092.
```

### Причина 2. Не вызывается `flush()`

Если producer отправляет records и процесс сразу завершается, сообщения могут остаться в локальном buffer.

Исправление в Python:

```python
producer.produce("test-events", key="k1", value='{"event":"test"}', callback=delivery_report)
producer.flush(10)
```

Ожидаемое поведение:

```text
flush ожидает доставку buffered records или timeout.
delivery callback сообщает topic, partition и offset успешной записи.
```

## 7. Consumer не читает records

### Симптом

```text
Consumer запущен, ошибок нет, records не появляются.
```

### Проверки

```bash
# Проверить наличие records через отдельную временную group.
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic test-events \
  --from-beginning \
  --group debug-reader
```

```bash
# Проверить offsets основной group.
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group app-consumer
```

Ожидаемый вывод:

```text
GROUP         TOPIC        PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
app-consumer  test-events  0          10              10              0
```

### Причина 1. Consumer group уже прочитала все records

Если `CURRENT-OFFSET` равен `LOG-END-OFFSET`, backlog отсутствует.

Исправление для повторного чтения:

```bash
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group app-consumer \
  --topic test-events \
  --reset-offsets \
  --to-earliest \
  --execute
```

Влияние:

```text
reset-offsets меняет сохраненную позицию group.
Команду нельзя выполнять без понимания последствий для прикладной обработки, потому что records могут быть обработаны повторно.
```

### Причина 2. `auto.offset.reset=latest`

Если group новая и `auto.offset.reset=latest`, consumer начнет читать только новые records после старта.

Исправление:

```properties
auto.offset.reset=earliest
```

Влияние:

```text
earliest читает с самого раннего доступного offset при отсутствии committed offset.
latest читает только новые records при отсутствии committed offset.
none завершает consumer ошибкой, если committed offset отсутствует.
```

### Причина 3. Несколько consumers в одной group и одна partition

Если topic имеет одну partition, только один consumer внутри group получит assignment. Остальные будут простаивать.

Проверка:

```bash
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group app-consumer \
  --members \
  --verbose
```

Исправление:

```text
Количество active consumers в group не может быть больше числа partitions topic.
Для параллельного чтения увеличить число partitions или уменьшить число consumers.
```

## 8. Consumer lag растет

### Симптом

```text
LOG-END-OFFSET увеличивается быстрее CURRENT-OFFSET.
LAG постоянно растет.
```

### Проверка

```bash
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group app-consumer
```

Ожидаемый плохой признак:

```text
LAG 10000
```

### Причины

- consumer обрабатывает records медленнее, чем producer пишет;
- мало partitions для нужного параллелизма;
- consumer делает медленную запись во внешнюю систему;
- частые rebalances;
- слишком маленькие `max.poll.records` или `fetch.min.bytes` не соответствуют профилю нагрузки;
- consumer падает до commit offset.

### Исправления

```text
Увеличить число consumers только если partitions достаточно.
Увеличить partitions для topic, если допустимо изменение key distribution.
Оптимизировать внешнюю запись: batch insert, idempotent upsert, connection pool.
Проверить ошибки consumer перед commit.
Проверить настройки max.poll.interval.ms и session.timeout.ms.
```

## 9. Частые rebalances consumer group

### Симптом

```text
Consumer постоянно получает revoke/assign partitions.
Обработка идет рывками.
Lag растет.
```

### Проверки

```bash
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group app-consumer \
  --members \
  --verbose
```

Проверка логов Python consumer:

```bash
grep -E 'rebalance|revoked|assigned|max.poll.interval|session.timeout' app-consumer.log
```

### Причина 1. Обработка дольше `max.poll.interval.ms`

Consumer должен регулярно вызывать `poll()`. Если обработка record занимает слишком много времени, broker считает consumer зависшим.

Исправление:

```text
Увеличить max.poll.interval.ms.
Уменьшить max.poll.records.
Вынести долгую обработку в worker pool с контролем commit.
```

### Причина 2. Нестабильная сеть или маленький `session.timeout.ms`

Исправление:

```text
Увеличить session.timeout.ms и heartbeat.interval.ms согласованно.
heartbeat.interval.ms обычно должен быть примерно в 3 раза меньше session.timeout.ms.
```

## 10. Multi-broker cluster потерял ISR

### Симптом

```text
Topic describe показывает Isr меньше Replicas.
```

### Проверка

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 \
  --describe \
  --topic orders
```

Ожидаемый нормальный вывод:

```text
Replicas: 1,2,3 Isr: 1,2,3
```

Плохой вывод:

```text
Replicas: 1,2,3 Isr: 1,2
```

### Причины

- broker 3 остановлен;
- broker 3 не может подключиться к leader;
- разные `cluster.id` после ошибочного форматирования storage;
- broker 3 имеет неверный `advertised.listeners`;
- диск broker 3 заполнен или недоступен.

### Исправления

```bash
# Проверить процесс broker.
sudo systemctl status kafka-3

# Проверить port broker.
ss -ltnp | grep 39092

# Проверить cluster id.
sudo grep cluster.id /var/lib/kafka/broker-3/meta.properties
```

Ожидаемое состояние:

```text
Все brokers одного cluster должны иметь одинаковый cluster.id.
node.id у каждого broker должен быть уникальным.
```

## 11. KRaft quorum недоступен

### Симптом

```text
Broker не может создать topic.
Metadata operations зависают.
```

### Проверка quorum

```bash
/opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  describe \
  --status
```

Ожидаемый вывод:

```text
LeaderId: 1
CurrentVoters: [1,2,3]
CurrentObservers: []
```

### Причины

- недоступно большинство controllers;
- неверный `controller.listener.names`;
- controllers слушают разные ports, не совпадающие с `controller.quorum.voters`;
- один из nodes отформатирован с другим `cluster.id`.

### Исправления

```text
Для quorum из 3 controllers должны работать минимум 2.
controller.quorum.voters должен быть одинаковым на всех nodes.
Каждый voter должен иметь корректный node.id@host:port.
```

## 12. Retention не удаляет старые records

### Симптом

```text
Records старше retention.ms остаются доступными.
Диск продолжает расти.
```

### Проверки

```bash
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name retention-test \
  --describe
```

```bash
sudo ls -lh /var/lib/kafka/kraft-combined-logs/retention-test-0
```

### Причины

- retention работает на segment files, а не на отдельных records;
- active segment еще не закрыт;
- `segment.ms` или `segment.bytes` слишком большие;
- topic-level config отличается от ожидаемого;
- `cleanup.policy=compact` без `delete` не удаляет records по времени как обычный delete-retention.

### Исправления

```bash
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name retention-test \
  --alter \
  --add-config retention.ms=60000,segment.ms=10000
```

Влияние:

```text
Маленький segment.ms чаще закрывает active segment, поэтому retention быстрее получает segments, которые можно удалить.
Слишком маленькие segments увеличивают количество файлов и нагрузку на broker.
```

## 13. Compaction не оставляет только последний key

### Симптом

```text
В compacted topic видны старые значения key.
```

### Проверки

```bash
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name user-state \
  --describe
```

Ожидаемый config:

```text
cleanup.policy=compact
```

### Причины

- compaction асинхронная и не выполняется мгновенно;
- active segment еще не закрыт;
- `min.cleanable.dirty.ratio` не достигнут;
- records отправлены без key;
- consumer читает topic с earliest и видит историю до compaction.

### Исправления

```text
Убедиться, что records имеют key.
Уменьшить segment.ms для стенда.
Уменьшить min.cleanable.dirty.ratio для ускорения compaction на тестовом topic.
Не использовать compaction как механизм немедленного удаления records.
```

## 14. TLS handshake падает

### Симптом

```text
SSL handshake failed
```

или:

```text
PKIX path building failed
```

### Проверки broker config

```bash
grep -E '^(listeners|listener.security.protocol.map|ssl.keystore.location|ssl.truststore.location|ssl.client.auth)=' /opt/kafka/config/kraft/server.properties
```

### Проверки client config

```bash
grep -E '^(security.protocol|ssl.ca.location|ssl.certificate.location|ssl.key.location|ssl.endpoint.identification.algorithm)=' client.properties
```

### Причины

- client не доверяет CA, которым подписан broker certificate;
- broker certificate не содержит hostname в SAN;
- client подключается по адресу, которого нет в certificate SAN;
- broker listener объявлен как SSL, а client использует PLAINTEXT;
- включен mutual TLS, но client certificate не передан.

### Исправления

```text
Добавить CA certificate в client trust settings.
Перевыпустить broker certificate с корректным Subject Alternative Name.
Использовать hostname, совпадающий с SAN.
Согласовать security.protocol client с listener.security.protocol.map broker.
Если ssl.client.auth=required, настроить client certificate и key.
```

## 15. ACL запрещает доступ

### Симптом

```text
TopicAuthorizationException
```

или:

```text
GroupAuthorizationException
```

### Проверка ACL

```bash
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server localhost:9092 \
  --list
```

### Проверка principal

В broker logs найти authenticated principal:

```bash
sudo journalctl -u kafka -n 200 --no-pager | grep -i principal
```

### Исправления для producer

```bash
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server localhost:9092 \
  --add \
  --allow-principal User:producer-app \
  --operation Write \
  --operation Describe \
  --topic orders
```

### Исправления для consumer

```bash
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server localhost:9092 \
  --add \
  --allow-principal User:consumer-app \
  --operation Read \
  --operation Describe \
  --topic orders

/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server localhost:9092 \
  --add \
  --allow-principal User:consumer-app \
  --operation Read \
  --group orders-reader
```

Влияние:

```text
Для чтения records consumer должен иметь Read на topic и Read на group.
Для записи producer должен иметь Write на topic.
Describe нужен многим clients для чтения metadata.
```

## 16. Kafka Connect worker не стартует

### Симптом

```text
Connect worker failed to start
```

### Проверки

```bash
grep -E '^(bootstrap.servers|group.id|config.storage.topic|offset.storage.topic|status.storage.topic|plugin.path)=' connect-distributed.properties
```

```bash
ls -la /opt/kafka/connect-plugins
```

### Причины

- worker не подключается к Kafka;
- internal topics не создаются из-за replication factor больше числа brokers;
- `plugin.path` не содержит connector plugins;
- connector jar несовместим с версией Kafka Connect;
- нет прав ACL на internal topics.

### Исправления

```properties
bootstrap.servers=localhost:9092
group.id=connect-cluster
config.storage.topic=connect-configs
offset.storage.topic=connect-offsets
status.storage.topic=connect-status
config.storage.replication.factor=1
offset.storage.replication.factor=1
status.storage.replication.factor=1
plugin.path=/opt/kafka/connect-plugins
```

Влияние:

```text
Для standalone broker replication factor internal topics должен быть 1.
Для production cluster replication factor internal topics обычно ставится 3.
```

## 17. PostgreSQL sink пишет дубли

### Симптом

```text
В таблице появляются повторяющиеся records после перезапуска consumer.
```

### Причина

Kafka consumer с at-least-once может обработать record повторно, если запись в PostgreSQL выполнена, но offset commit не успел завершиться.

### Исправление

Использовать idempotency key и `ON CONFLICT`.

```sql
CREATE TABLE processed_orders (
    event_id text PRIMARY KEY,
    order_id text NOT NULL,
    amount numeric NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO processed_orders (event_id, order_id, amount)
VALUES ($1, $2, $3)
ON CONFLICT (event_id) DO NOTHING;
```

Правильный порядок:

```text
poll Kafka record -> write PostgreSQL with idempotency key -> commit Kafka offset
```

## 18. Prometheus не видит Kafka metrics

### Симптом

```text
Prometheus target down.
Grafana panels пустые.
```

### Проверки

```bash
curl -sS http://localhost:7071/metrics | head
```

Ожидаемый вывод:

```text
# HELP kafka_...
# TYPE kafka_...
```

Проверка Prometheus targets:

```bash
curl -sS http://localhost:9090/api/v1/targets
```

### Причины

- JMX exporter не подключен к JVM Kafka;
- port 7071 не слушается;
- Prometheus scrape config содержит неверный host или port;
- firewall блокирует scrape;
- Grafana datasource указывает не на тот Prometheus.

### Исправления

```text
Проверить KAFKA_OPTS с javaagent jmx_prometheus_javaagent.
Проверить port 7071 через ss -ltnp.
Проверить prometheus.yml.
Проверить targets в Prometheus UI или через API.
```

## 19. Диск broker быстро заполняется

### Симптом

```text
df -h показывает быстрый рост /var/lib/kafka.
Broker logs содержит ошибки записи на диск.
```

### Проверки

```bash
df -h /var/lib/kafka
sudo du -h --max-depth=2 /var/lib/kafka | sort -h | tail -20
```

### Причины

- большой retention;
- много partitions;
- высокий replication factor;
- compacted topics без delete-retention;
- producer пишет быстрее, чем ожидалось;
- internal topics или тестовые topics не удалены.

### Исправления

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name large-topic \
  --alter \
  --add-config retention.ms=86400000,retention.bytes=1073741824
```

Влияние:

```text
retention.bytes применяется на partition.
Topic с 10 partitions и retention.bytes=1GB может занимать до 10GB до учета replicas.
При replication factor 3 физический объем может достигать примерно 30GB.
```

## 20. Быстрые команды диагностики

```bash
# Процесс Kafka.
sudo systemctl status kafka

# Последние логи Kafka.
sudo journalctl -u kafka -n 100 --no-pager

# Listener.
ss -ltnp | grep 9092

# Kafka protocol.
/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# KRaft quorum.
/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status

# Список topics.
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Описание topic.
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic test-events

# Consumer groups.
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# Consumer lag.
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group app-consumer

# Topic configs.
/opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name test-events --describe

# ACL.
/opt/kafka/bin/kafka-acls.sh --bootstrap-server localhost:9092 --list

# Kafka data size.
sudo du -h --max-depth=2 /var/lib/kafka | sort -h | tail -20
```

