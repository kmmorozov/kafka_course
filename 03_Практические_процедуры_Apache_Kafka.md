# Практические процедуры Apache Kafka

## Общие обозначения

```bash
# Каталог Kafka.
export KAFKA_HOME=/opt/kafka

# Версия Kafka для установки из Apache archive.
export KAFKA_VERSION=3.7.2

# Scala binary version для Kafka 3.x.
export SCALA_VERSION=2.13

# Каталог Python-приложений.
export PY_APP_DIR=/home/student/kafka-course-python
```

Конфиги приведены как рабочие фрагменты для выполнения процедур. Полные версии конфигов с комментариями по каждому параметру, возможными значениями и влиянием изменения находятся в [04_Справочник_конфигураций.md](04_Справочник_конфигураций.md).

## Практическая процедура 1. Установка Kafka и standalone broker

### Назначение

Развернуть один Kafka broker в режиме KRaft без ZooKeeper, проверить доступность client listener и создать первый topic. После работы на стенде должен быть работающий процесс Kafka, который принимает admin-команды, producer-записи и consumer-чтение.

### Технический сценарий

На чистой Rocky Linux устанавливается Java, создается системный пользователь `kafka`, распаковывается Kafka в `/opt/kafka`, подготавливается каталог данных `/var/lib/kafka/kraft-combined-logs`. Broker запускается в combined mode: один процесс одновременно выполняет роль `broker` и `controller`. Metadata кластера хранится в KRaft metadata log, поэтому перед первым запуском выполняется `kafka-storage.sh format`.

### Проверяемое состояние

- Java доступна для запуска Kafka.
- Kafka scripts доступны в `/opt/kafka/bin`.
- KRaft storage отформатирован одним `cluster.id`.
- Broker слушает `localhost:9092`.
- Admin API отвечает через `kafka-broker-api-versions.sh`.
- Topic `test-events` создается с одной partition и replication factor 1.

### 1. Установка Java

```bash
# Установка Java runtime и development kit.
sudo dnf install -y java-17-openjdk java-17-openjdk-devel

# Проверка версии Java.
java -version
```

Ожидаемый вывод:

```text
openjdk version "17...
OpenJDK Runtime Environment ...
OpenJDK 64-Bit Server VM ...
```

### 2. Создание пользователя и каталогов

```bash
# Системный пользователь для запуска Kafka.
sudo useradd --system --home-dir /opt/kafka --shell /sbin/nologin kafka

# Каталоги данных и логов.
sudo mkdir -p /var/lib/kafka/kraft-combined-logs /var/log/kafka
```

Пояснение:

```text
/var/lib/kafka/kraft-combined-logs хранит partitions и metadata log KRaft.
/var/log/kafka используется для логов процесса, если запуск оформляется через systemd.
```

### 3. Загрузка и распаковка Kafka

```bash
# Загрузка архива из стабильного Apache archive.
curl -LO "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"

# Распаковка в /opt.
sudo tar -xzf "kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" -C /opt

# Стабильная ссылка /opt/kafka.
sudo ln -sfn "/opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" /opt/kafka

# Назначение владельца.
sudo chown -R kafka:kafka /opt/kafka "/opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" /var/lib/kafka /var/log/kafka
```

Ожидаемый результат:

```bash
# Проверка исполняемых файлов Kafka.
ls /opt/kafka/bin/kafka-server-start.sh
```

```text
/opt/kafka/bin/kafka-server-start.sh
```

### 4. Конфиг standalone broker

Файл `/opt/kafka/config/kraft/server.properties`:

```properties
# Уникальный идентификатор узла Kafka.
node.id=1

# Один процесс выполняет роли broker и controller.
process.roles=broker,controller

# Единственный controller voter в standalone KRaft.
controller.quorum.voters=1@localhost:9093

# Client listener и controller listener.
listeners=PLAINTEXT://:9092,CONTROLLER://:9093

# Адрес, который broker возвращает клиентам.
advertised.listeners=PLAINTEXT://localhost:9092

# Listener для controller quorum.
controller.listener.names=CONTROLLER

# Протоколы listeners.
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

# Межброкерный listener.
inter.broker.listener.name=PLAINTEXT

# Каталог данных.
log.dirs=/var/lib/kafka/kraft-combined-logs

# Количество partitions по умолчанию.
num.partitions=1

# Внутренние topics в single-broker режиме.
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
```

### 5. Форматирование хранилища KRaft

```bash
# Генерация cluster id.
KAFKA_CLUSTER_ID="$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)"

# Форматирование log.dirs.
sudo -u kafka "$KAFKA_HOME/bin/kafka-storage.sh" format \
  -t "$KAFKA_CLUSTER_ID" \
  -c "$KAFKA_HOME/config/kraft/server.properties"
```

Ожидаемый вывод:

```text
Formatting /var/lib/kafka/kraft-combined-logs with metadata.version ...
```

Пояснение:

```text
format записывает meta.properties и metadata log.
Повторный format с другим cluster.id для непустого каталога вызовет ошибку.
```

### 6. Запуск broker

```bash
# Запуск broker в foreground.
sudo -u kafka "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_HOME/config/kraft/server.properties"
```

Ожидаемые строки в логе:

```text
[KafkaRaftServer nodeId=1] Kafka Server started
started (kafka.server.KafkaRaftServer)
```

### 7. Проверка broker

Во втором терминале:

```bash
# Проверка доступных Kafka API на broker.
"$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server localhost:9092
```

Ожидаемый вывод:

```text
localhost:9092 (id: 1 rack: null) -> (
  Produce(0): ...
  Fetch(1): ...
```

### 8. Создание тестового topic

```bash
# Создание topic test-events.
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --create \
  --topic test-events \
  --partitions 1 \
  --replication-factor 1
```

Ожидаемый вывод:

```text
Created topic test-events.
```

## Практическая процедура 2. Topics, partitions, keys, offsets

### Назначение

Создать topic с несколькими partitions, отправить сообщения с ключами, прочитать сообщения с отображением partition и offset. После работы должно быть понятно, что порядок сообщений гарантируется внутри partition, а key влияет на выбор partition.

### Технический сценарий

Topic `orders` создается с тремя partitions на standalone broker. Console producer получает строки в формате `key:value`, Kafka выбирает partition по hash от key. Console consumer читает сообщения с начала topic и печатает key, partition и offset. Затем проверяется committed offset consumer group через `kafka-consumer-groups.sh`.

### Проверяемое состояние

- Topic metadata содержит три partitions.
- Сообщения с одинаковым key попадают в одну partition.
- Offset растет независимо внутри каждой partition.
- Consumer group хранит позицию чтения отдельно от topic.
- `LAG` становится 0 после чтения всех сообщений.

### 1. Создание topic с тремя partitions

```bash
# Topic orders содержит три независимых partition log.
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 1
```

Ожидаемый вывод:

```text
Created topic orders.
```

### 2. Описание topic

```bash
# Просмотр leader, replicas и ISR.
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders
```

Ожидаемый вывод:

```text
Topic: orders TopicId: ... PartitionCount: 3 ReplicationFactor: 1 Configs:
  Topic: orders Partition: 0 Leader: 1 Replicas: 1 Isr: 1
  Topic: orders Partition: 1 Leader: 1 Replicas: 1 Isr: 1
  Topic: orders Partition: 2 Leader: 1 Replicas: 1 Isr: 1
```

### 3. Отправка сообщений через console producer

```bash
# Producer читает key:value из stdin.
"$KAFKA_HOME/bin/kafka-console-producer.sh" \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --property parse.key=true \
  --property key.separator=:
```

Ввод:

```text
customer-1:{"order_id":1,"amount":100}
customer-2:{"order_id":2,"amount":200}
customer-1:{"order_id":3,"amount":300}
```

Пояснение:

```text
parse.key=true берет часть до ':' как key.
Hash key выбирает partition.
Сообщения customer-1 должны попасть в одну partition.
```

### 4. Чтение с печатью metadata

```bash
# Consumer печатает key, partition и offset.
"$KAFKA_HOME/bin/kafka-console-consumer.sh" \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group orders-lab-reader \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true
```

Ожидаемый вывод:

```text
Partition:1 Offset:0 customer-1 {"order_id":1,"amount":100}
Partition:0 Offset:0 customer-2 {"order_id":2,"amount":200}
Partition:1 Offset:1 customer-1 {"order_id":3,"amount":300}
```

Номера partitions могут отличаться. У одинакового key partition должна совпадать.

### 5. Просмотр consumer group offset

```bash
# Проверка committed offsets и lag.
"$KAFKA_HOME/bin/kafka-consumer-groups.sh" \
  --bootstrap-server localhost:9092 \
  --describe \
  --group orders-lab-reader
```

Ожидаемый вывод:

```text
GROUP              TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
orders-lab-reader  orders  0          1               1               0
orders-lab-reader  orders  1          2               2               0
orders-lab-reader  orders  2          0               0               0
```

## Практическая процедура 3. Python producer и consumer с ручным commit

### Назначение

Реализовать producer и consumer на Python, отправить JSON-сообщения в Kafka, обработать их consumer group и зафиксировать offsets вручную после обработки. После работы должен быть готов базовый шаблон Python-приложений для последующих интеграционных сценариев.

### Технический сценарий

Python producer формирует события заказов, использует `customer_id` как key и отправляет JSON в topic `orders`. Python consumer входит в group `orders-python-workers`, читает сообщения, декодирует JSON, имитирует обработку и вызывает `commit()` только после успешной обработки. При запуске двух consumers Kafka распределяет partitions между экземплярами внутри одной group.

### Проверяемое состояние

- Python client `confluent-kafka` подключается к Kafka broker.
- Producer получает delivery callback с partition и offset.
- Consumer читает JSON payload и metadata сообщения.
- Manual commit фиксирует offset после обработки.
- Две копии consumer делят partitions одной group.
- Повторный запуск consumer не перечитывает уже committed сообщения.

### 1. Python environment

```bash
# Создание каталога приложений.
mkdir -p "$PY_APP_DIR"
cd "$PY_APP_DIR"

# Создание venv.
python3 -m venv .venv

# Активация venv.
source .venv/bin/activate

# Установка Python Kafka client и dotenv.
pip install confluent-kafka python-dotenv
```

Ожидаемый вывод:

```text
Successfully installed confluent-kafka-... python-dotenv-...
```

### 2. Конфиг окружения

Файл `.env`:

```dotenv
# Bootstrap server standalone Kafka.
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Topic для заказов.
ORDERS_TOPIC=orders
```

### 3. Python producer

Файл `orders_producer.py`:

```python
# Producer отправляет JSON-сообщения с key=customer_id.
import json
import os
import sys

from confluent_kafka import Producer
from dotenv import load_dotenv

# Загрузка .env из текущего каталога.
load_dotenv()

# Конфиг Kafka producer.
producer = Producer(
    {
        # Kafka bootstrap server.
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        # Подтверждение записи всеми ISR replicas.
        "acks": "all",
        # Защита от дублей при retry.
        "enable.idempotence": True,
        # Сжатие batch.
        "compression.type": "lz4",
    }
)


def delivery_report(error, message):
    # Callback печатает результат доставки.
    if error:
        print(f"delivery failed: {error}", file=sys.stderr)
        return
    print(
        f"delivered key={message.key().decode()} "
        f"partition={message.partition()} offset={message.offset()}"
    )


topic = os.environ["ORDERS_TOPIC"]

for order_id in range(1, 11):
    customer_id = f"customer-{order_id % 3}"
    event = {
        "order_id": order_id,
        "customer_id": customer_id,
        "amount": order_id * 100,
    }
    producer.produce(
        topic=topic,
        key=customer_id,
        value=json.dumps(event),
        callback=delivery_report,
    )
    # poll обрабатывает delivery callbacks.
    producer.poll(0)

# flush дожидается доставки всех сообщений.
producer.flush()
```

### 4. Python consumer

Файл `orders_consumer.py`:

```python
# Consumer читает заказы и фиксирует offset после обработки.
import json
import os
import time

from confluent_kafka import Consumer
from dotenv import load_dotenv

# Загрузка .env.
load_dotenv()

# Конфиг Kafka consumer.
consumer = Consumer(
    {
        # Kafka bootstrap server.
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        # Group id для балансировки partitions.
        "group.id": "orders-python-workers",
        # Новая group читает topic с начала.
        "auto.offset.reset": "earliest",
        # Автоматический commit выключен.
        "enable.auto.commit": False,
    }
)

consumer.subscribe([os.environ["ORDERS_TOPIC"]])

try:
    while True:
        # poll ожидает сообщение до 1 секунды.
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"consumer error: {msg.error()}")
            continue

        event = json.loads(msg.value().decode("utf-8"))
        print(
            f"processed order_id={event['order_id']} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )

        # Имитация прикладной обработки.
        time.sleep(0.2)

        # Commit offset после обработки.
        consumer.commit(message=msg)
except KeyboardInterrupt:
    pass
finally:
    # close выполняет LeaveGroup.
    consumer.close()
```

### 5. Запуск producer

```bash
# Запуск producer.
python orders_producer.py
```

Ожидаемый вывод:

```text
delivered key=customer-1 partition=... offset=...
delivered key=customer-2 partition=... offset=...
delivered key=customer-0 partition=... offset=...
```

### 6. Запуск двух consumers

Терминал 1:

```bash
cd "$PY_APP_DIR"
source .venv/bin/activate
python orders_consumer.py
```

Терминал 2:

```bash
cd "$PY_APP_DIR"
source .venv/bin/activate
python orders_consumer.py
```

Ожидаемый вывод:

```text
processed order_id=... partition=0 offset=...
processed order_id=... partition=1 offset=...
```

Пояснение:

```text
Одна consumer group получает каждое сообщение один раз.
Partitions распределяются между активными consumers.
Если consumers больше, чем partitions, часть consumers простаивает.
```

### 7. Проверка group lag

```bash
"$KAFKA_HOME/bin/kafka-consumer-groups.sh" \
  --bootstrap-server localhost:9092 \
  --describe \
  --group orders-python-workers
```

Ожидаемый вывод после обработки:

```text
GROUP                 TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
orders-python-workers orders  0          ...             ...             0
orders-python-workers orders  1          ...             ...             0
orders-python-workers orders  2          ...             ...             0
```

## Практическая процедура 4. Кластер из трех brokers

### Назначение

Развернуть Kafka cluster из трех brokers в KRaft combined mode, создать topic с replication factor 3 и проверить поведение ISR и leader election при остановке одного broker. После работы должен быть рабочий отказоустойчивый кластер.

### Технический сценарий

На одном сервере запускаются три процесса Kafka с разными `node.id`, client ports, controller ports и каталогами данных. Все три процесса используют один `cluster.id` и одинаковый `controller.quorum.voters`. Topic `replicated-orders` создается с тремя partitions и тремя replicas. После остановки одного broker проверяется, что Kafka выбирает новых leaders и продолжает обслуживать topic при достаточном ISR.

### Проверяемое состояние

- Три brokers входят в один KRaft cluster.
- `Replicas` содержит три broker id для каждой partition.
- `Isr` содержит три broker id до отказа и два после остановки одного broker.
- `Leader` меняется для partitions, leader которых был на остановленном broker.
- Producer с `acks=all` продолжает писать, пока ISR не меньше `min.insync.replicas`.

### 1. Каталоги данных

```bash
# Каталоги трех brokers.
sudo mkdir -p /var/lib/kafka/broker-1 /var/lib/kafka/broker-2 /var/lib/kafka/broker-3

# Владелец каталогов.
sudo chown -R kafka:kafka /var/lib/kafka/broker-1 /var/lib/kafka/broker-2 /var/lib/kafka/broker-3
```

### 2. Конфиг broker 1

Файл `/opt/kafka/config/kraft/broker-1.properties`:

```properties
# Идентификатор первого узла.
node.id=1

# Узел является broker и controller.
process.roles=broker,controller

# Controller quorum из трех узлов.
controller.quorum.voters=1@localhost:19093,2@localhost:29093,3@localhost:39093

# Client и controller listeners первого узла.
listeners=PLAINTEXT://:19092,CONTROLLER://:19093

# Адрес первого broker для клиентов.
advertised.listeners=PLAINTEXT://localhost:19092

# Listener controller quorum.
controller.listener.names=CONTROLLER

# Карта listener protocol.
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

# Межброкерный listener.
inter.broker.listener.name=PLAINTEXT

# Данные первого broker.
log.dirs=/var/lib/kafka/broker-1

# Внутренние topics реплицируются на три broker.
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

# Partitions по умолчанию.
num.partitions=3
```

### 3. Конфиги broker 2 и broker 3

Файл `/opt/kafka/config/kraft/broker-2.properties`:

```properties
# Идентификатор второго узла.
node.id=2

# Узел является broker и controller.
process.roles=broker,controller

# Controller quorum из трех узлов.
controller.quorum.voters=1@localhost:19093,2@localhost:29093,3@localhost:39093

# Client и controller listeners второго узла.
listeners=PLAINTEXT://:29092,CONTROLLER://:29093

# Адрес второго broker для клиентов.
advertised.listeners=PLAINTEXT://localhost:29092

# Listener controller quorum.
controller.listener.names=CONTROLLER

# Карта listener protocol.
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

# Межброкерный listener.
inter.broker.listener.name=PLAINTEXT

# Данные второго broker.
log.dirs=/var/lib/kafka/broker-2

# Внутренние topics реплицируются на три broker.
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

# Partitions по умолчанию.
num.partitions=3
```

Файл `/opt/kafka/config/kraft/broker-3.properties`:

```properties
# Идентификатор третьего узла.
node.id=3

# Узел является broker и controller.
process.roles=broker,controller

# Controller quorum из трех узлов.
controller.quorum.voters=1@localhost:19093,2@localhost:29093,3@localhost:39093

# Client и controller listeners третьего узла.
listeners=PLAINTEXT://:39092,CONTROLLER://:39093

# Адрес третьего broker для клиентов.
advertised.listeners=PLAINTEXT://localhost:39092

# Listener controller quorum.
controller.listener.names=CONTROLLER

# Карта listener protocol.
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

# Межброкерный listener.
inter.broker.listener.name=PLAINTEXT

# Данные третьего broker.
log.dirs=/var/lib/kafka/broker-3

# Внутренние topics реплицируются на три broker.
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

# Partitions по умолчанию.
num.partitions=3
```

### 4. Форматирование трех brokers

```bash
# Один cluster.id для всех узлов кластера.
KAFKA_CLUSTER_ID="$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)"

# Broker 1.
sudo -u kafka "$KAFKA_HOME/bin/kafka-storage.sh" format \
  -t "$KAFKA_CLUSTER_ID" \
  -c "$KAFKA_HOME/config/kraft/broker-1.properties"

# Broker 2.
sudo -u kafka "$KAFKA_HOME/bin/kafka-storage.sh" format \
  -t "$KAFKA_CLUSTER_ID" \
  -c "$KAFKA_HOME/config/kraft/broker-2.properties"

# Broker 3.
sudo -u kafka "$KAFKA_HOME/bin/kafka-storage.sh" format \
  -t "$KAFKA_CLUSTER_ID" \
  -c "$KAFKA_HOME/config/kraft/broker-3.properties"
```

Ожидаемый вывод:

```text
Formatting /var/lib/kafka/broker-1 with metadata.version ...
Formatting /var/lib/kafka/broker-2 with metadata.version ...
Formatting /var/lib/kafka/broker-3 with metadata.version ...
```

### 5. Запуск трех brokers

Терминал 1:

```bash
sudo -u kafka "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_HOME/config/kraft/broker-1.properties"
```

Терминал 2:

```bash
sudo -u kafka "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_HOME/config/kraft/broker-2.properties"
```

Терминал 3:

```bash
sudo -u kafka "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_HOME/config/kraft/broker-3.properties"
```

Ожидаемый признак:

```text
Все три процесса содержат Kafka Server started.
Порты 19092, 29092, 39092 слушаются.
```

### 6. Replicated topic

```bash
# Topic с replication factor 3.
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:19092 \
  --create \
  --topic replicated-orders \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2

# Описание topic.
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:19092 \
  --describe \
  --topic replicated-orders
```

Ожидаемый вывод:

```text
Topic: replicated-orders PartitionCount: 3 ReplicationFactor: 3
  Partition: 0 Leader: ... Replicas: 1,2,3 Isr: 1,2,3
  Partition: 1 Leader: ... Replicas: 2,3,1 Isr: 2,3,1
  Partition: 2 Leader: ... Replicas: 3,1,2 Isr: 3,1,2
```

### 7. Проверка отказоустойчивости

```bash
# Отправка сообщения с acks=all.
"$KAFKA_HOME/bin/kafka-console-producer.sh" \
  --bootstrap-server localhost:19092,localhost:29092,localhost:39092 \
  --topic replicated-orders \
  --producer-property acks=all
```

Ввод:

```text
before-failure
```

Остановить один broker клавишами `Ctrl+C` в одном из терминалов broker.

```bash
# Повторное описание topic после остановки broker.
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:19092,localhost:29092,localhost:39092 \
  --describe \
  --topic replicated-orders
```

Ожидаемый вывод:

```text
Isr содержит два broker вместо трех.
Leader для partitions на остановленном broker перемещен на доступный broker.
```

## Практическая процедура 5. Retention, segments, compaction

### Назначение

Проверить, как Kafka хранит сообщения на диске, как закрываются log segments, как работает удаление по retention и как compacted topic сохраняет последнее значение для каждого key. После работы должно быть понятно, что Kafka удаляет не отдельные сообщения, а segment files, и что compaction работает по ключам.

### Технический сценарий

Создаются два topic. `retention-demo` использует короткое время хранения и маленькие segments, чтобы увидеть файлы partition log и их удаление. `compact-demo` использует `cleanup.policy=compact`; в него отправляются несколько значений с одинаковым key, затем topic перечитывается с начала до и после работы log cleaner.

### Проверяемое состояние

- Partition хранится как каталог с `.log`, `.index`, `.timeindex`.
- Retention применяется к closed segments, active segment не удаляется сразу.
- `segment.ms` и `segment.bytes` влияют на скорость закрытия segment.
- Compaction оставляет последнее значение для key.
- Tombstone и `delete.retention.ms` используются для удаления key в compacted topics.

### 1. Topic с коротким retention

```bash
# Topic с маленьким segment и коротким retention.
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --create \
  --topic retention-demo \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=60000 \
  --config segment.ms=10000 \
  --config segment.bytes=1048576
```

Ожидаемый вывод:

```text
Created topic retention-demo.
```

### 2. Отправка данных

```bash
# Producer для заполнения topic.
for i in $(seq 1 1000); do
  echo "event-$i"
done | "$KAFKA_HOME/bin/kafka-console-producer.sh" \
  --bootstrap-server localhost:9092 \
  --topic retention-demo
```

Пояснение:

```text
segment.ms=10000 позволяет быстрее закрывать segments.
retention.ms=60000 удаляет закрытые segments старше 60 секунд.
```

### 3. Проверка segment files

```bash
# Поиск файлов partition retention-demo-0.
sudo find /var/lib/kafka -path '*retention-demo-0*' -type f -maxdepth 5
```

Ожидаемый вывод:

```text
/var/lib/kafka/.../retention-demo-0/00000000000000000000.log
/var/lib/kafka/.../retention-demo-0/00000000000000000000.index
/var/lib/kafka/.../retention-demo-0/00000000000000000000.timeindex
```

Через 1-2 минуты часть старых closed segments удаляется. Active segment не удаляется, даже если сообщения в нем старше retention.

### 4. Compacted topic

```bash
# Topic с log compaction.
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --create \
  --topic compact-demo \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=compact \
  --config segment.ms=10000 \
  --config min.cleanable.dirty.ratio=0.01 \
  --config delete.retention.ms=30000
```

Ожидаемый вывод:

```text
Created topic compact-demo.
```

### 5. Отправка нескольких значений на один key

```bash
# Отправка key:value.
"$KAFKA_HOME/bin/kafka-console-producer.sh" \
  --bootstrap-server localhost:9092 \
  --topic compact-demo \
  --property parse.key=true \
  --property key.separator=:
```

Ввод:

```text
user-1:{"status":"created"}
user-1:{"status":"active"}
user-2:{"status":"created"}
user-1:{"status":"blocked"}
```

Пояснение:

```text
После compaction для user-1 остается последнее значение {"status":"blocked"}.
Compaction не обязан выполняться немедленно.
```

### 6. Проверка чтения с начала

```bash
"$KAFKA_HOME/bin/kafka-console-consumer.sh" \
  --bootstrap-server localhost:9092 \
  --topic compact-demo \
  --from-beginning \
  --property print.key=true \
  --timeout-ms 10000
```

Ожидаемый вывод до compaction:

```text
user-1 {"status":"created"}
user-1 {"status":"active"}
user-2 {"status":"created"}
user-1 {"status":"blocked"}
```

Ожидаемый вывод после compaction:

```text
user-2 {"status":"created"}
user-1 {"status":"blocked"}
```

## Практическая процедура 6. PostgreSQL и Kafka через Python

### Назначение

Построить внешний поток данных на Python: читать новые строки из PostgreSQL, публиковать их в Kafka и записывать прочитанные события обратно в PostgreSQL. После работы должен быть понятен базовый шаблон интеграции Kafka с внешней системой без Kafka Connect.

### Технический сценарий

В PostgreSQL создаются таблицы `orders` и `processed_orders`. Python source-приложение периодически выбирает новые строки из `orders` по возрастающему `id` и отправляет JSON-события в topic `pg_orders`. Python sink-приложение читает `pg_orders`, выполняет UPSERT в `processed_orders` и фиксирует Kafka offset только после успешного commit в PostgreSQL.

### Проверяемое состояние

- PostgreSQL доступен Python-приложениям через `psycopg`.
- Source-приложение преобразует SQL rows в Kafka events.
- Sink-приложение реализует at-least-once обработку.
- `ON CONFLICT DO UPDATE` делает повторную обработку безопасной.
- Offset commit после database commit предотвращает потерю события между Kafka и PostgreSQL.
- Строка из `orders` появляется в `processed_orders`.

### 1. Установка PostgreSQL

```bash
# Установка PostgreSQL server.
sudo dnf install -y postgresql-server postgresql-contrib

# Инициализация базы.
sudo postgresql-setup --initdb

# Запуск PostgreSQL.
sudo systemctl enable --now postgresql
```

Ожидаемый вывод:

```text
Initialized database in ...
Created symlink ...
```

### 2. Схема данных

```bash
# Создание пользователя и базы.
sudo -u postgres psql
```

SQL:

```sql
-- Пользователь для Python-приложений.
CREATE USER kafka WITH PASSWORD 'kafka';

-- База данных PostgreSQL/Kafka pipeline.
CREATE DATABASE kafka_lab OWNER kafka;

-- Подключение к базе.
\c kafka_lab

-- Таблица-источник.
CREATE TABLE orders (
    id BIGSERIAL PRIMARY KEY,
    customer_id TEXT NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Таблица-приемник.
CREATE TABLE processed_orders (
    id BIGINT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

-- Права на таблицы.
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO kafka;

-- Права на sequence orders_id_seq.
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO kafka;
```

Ожидаемый вывод:

```text
CREATE ROLE
CREATE DATABASE
CREATE TABLE
GRANT
```

### 3. Kafka topic

```bash
# Topic для строк orders из PostgreSQL.
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --create \
  --topic pg_orders \
  --partitions 3 \
  --replication-factor 1
```

### 4. Python зависимости

```bash
cd "$PY_APP_DIR"
source .venv/bin/activate

# PostgreSQL client и Kafka client.
pip install confluent-kafka psycopg[binary] python-dotenv
```

Файл `.env`:

```dotenv
# Kafka bootstrap server.
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Topic для PostgreSQL source events.
KAFKA_TOPIC=pg_orders

# PostgreSQL DSN.
POSTGRES_DSN=postgresql://kafka:kafka@localhost:5432/kafka_lab
```

### 5. Python source: PostgreSQL -> Kafka

Файл `pg_orders_producer.py`:

```python
# Source-приложение читает новые строки PostgreSQL и отправляет их в Kafka.
import json
import os
import time

import psycopg
from confluent_kafka import Producer
from dotenv import load_dotenv

# Загрузка .env.
load_dotenv()

# Kafka producer config.
producer = Producer(
    {
        # Kafka broker.
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        # Подтверждение всеми ISR.
        "acks": "all",
        # Идемпотентность producer.
        "enable.idempotence": True,
    }
)


def report(error, message):
    # Callback доставки.
    if error:
        print(f"delivery failed: {error}")
    else:
        print(f"delivered id={message.key().decode()} offset={message.offset()}")


last_id = 0
topic = os.environ["KAFKA_TOPIC"]
dsn = os.environ["POSTGRES_DSN"]

while True:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            # Выбор новых строк по id.
            cur.execute(
                """
                SELECT id, customer_id, amount, status, created_at
                FROM orders
                WHERE id > %s
                ORDER BY id
                """,
                (last_id,),
            )
            rows = cur.fetchall()

    for row in rows:
        order_id, customer_id, amount, status, created_at = row
        event = {
            "id": order_id,
            "customer_id": customer_id,
            "amount": float(amount),
            "status": status,
            "created_at": created_at.isoformat(),
        }
        producer.produce(
            topic,
            key=str(order_id),
            value=json.dumps(event),
            callback=report,
        )
        last_id = order_id

    producer.poll(0)
    time.sleep(2)
```

### 6. Python sink: Kafka -> PostgreSQL

Файл `pg_orders_sink.py`:

```python
# Sink-приложение читает Kafka и пишет в PostgreSQL.
import json
import os

import psycopg
from confluent_kafka import Consumer
from dotenv import load_dotenv

# Загрузка .env.
load_dotenv()

# Kafka consumer config.
consumer = Consumer(
    {
        # Kafka broker.
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        # Consumer group sink-приложения.
        "group.id": "pg-orders-sink",
        # Новая group читает с начала.
        "auto.offset.reset": "earliest",
        # Commit offset вручную после записи в PostgreSQL.
        "enable.auto.commit": False,
    }
)

consumer.subscribe([os.environ["KAFKA_TOPIC"]])

with psycopg.connect(os.environ["POSTGRES_DSN"]) as conn:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"consumer error: {msg.error()}")
            continue

        event = json.loads(msg.value().decode("utf-8"))
        with conn.cursor() as cur:
            # UPSERT сохраняет идемпотентность sink.
            cur.execute(
                """
                INSERT INTO processed_orders(id, customer_id, amount, status, created_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET customer_id = EXCLUDED.customer_id,
                    amount = EXCLUDED.amount,
                    status = EXCLUDED.status,
                    created_at = EXCLUDED.created_at
                """,
                (
                    event["id"],
                    event["customer_id"],
                    event["amount"],
                    event["status"],
                    event["created_at"],
                ),
            )
        conn.commit()
        consumer.commit(message=msg)
        print(f"stored id={event['id']} offset={msg.offset()}")
```

### 7. Запуск и проверка

Терминал 1:

```bash
cd "$PY_APP_DIR"
source .venv/bin/activate
python pg_orders_producer.py
```

Терминал 2:

```bash
cd "$PY_APP_DIR"
source .venv/bin/activate
python pg_orders_sink.py
```

Терминал 3:

```bash
# Вставка строки в orders.
psql "postgresql://kafka:kafka@localhost:5432/kafka_lab" \
  -c "INSERT INTO orders(customer_id, amount, status) VALUES ('customer-1', 1500.00, 'created');"

# Проверка sink-таблицы.
psql "postgresql://kafka:kafka@localhost:5432/kafka_lab" \
  -c "SELECT id, customer_id, amount, status FROM processed_orders ORDER BY id;"
```

Ожидаемый вывод producer:

```text
delivered id=1 offset=0
```

Ожидаемый вывод sink:

```text
stored id=1 offset=0
```

Ожидаемый вывод SQL:

```text
 id | customer_id | amount  | status
----+-------------+---------+---------
  1 | customer-1  | 1500.00 | created
```

## Практическая процедура 7. Потоковая обработка через Python

### Назначение

Собрать упрощенный stream processing pipeline на Python: генерировать телеметрию, читать поток событий, агрегировать события по устройству и публиковать агрегаты в отдельный topic. После работы должен быть понятен принцип streaming topology: input topic, processing application, output topic.

### Технический сценарий

Python generator публикует события устройств в topic `telemetry`, используя `device_id` как key. Python aggregator читает `telemetry`, считает количество событий по `device_id` в минутном окне и записывает результат в `telemetry-1m`. Console consumer читает output topic и выводит агрегаты.

### Проверяемое состояние

- Key `device_id` сохраняет порядок событий одного устройства внутри partition.
- Stream processor одновременно является consumer и producer.
- Aggregation требует состояния в памяти приложения.
- Output topic содержит производные события, а не исходные измерения.
- При сбое до commit входные события могут быть обработаны повторно.

### 1. Topics

```bash
# Topic сырых событий.
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --create \
  --topic telemetry \
  --partitions 3 \
  --replication-factor 1

# Topic агрегатов.
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --create \
  --topic telemetry-1m \
  --partitions 3 \
  --replication-factor 1
```

### 2. Генератор телеметрии

Файл `telemetry_generator.py`:

```python
# Генератор отправляет события устройств в Kafka.
import json
import os
import random
import time

from confluent_kafka import Producer
from dotenv import load_dotenv

# Загрузка .env.
load_dotenv()

# Kafka producer config.
producer = Producer(
    {
        # Kafka broker.
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        # Быстрое подтверждение от leader.
        "acks": "1",
    }
)

topic = "telemetry"
devices = ["sensor-1", "sensor-2", "sensor-3"]

while True:
    device_id = random.choice(devices)
    event = {
        "device_id": device_id,
        "temperature": round(random.uniform(20.0, 35.0), 2),
        "ts": time.time(),
    }
    producer.produce(
        topic,
        key=device_id,
        value=json.dumps(event),
    )
    producer.poll(0)
    print(event)
    time.sleep(0.5)
```

### 3. Aggregator

Файл `telemetry_aggregator.py`:

```python
# Aggregator считает количество событий по device_id в минутном окне.
import json
import os
import time
from collections import defaultdict

from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

# Загрузка .env.
load_dotenv()

# Consumer config.
consumer = Consumer(
    {
        # Kafka broker.
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        # Group stream processor.
        "group.id": "telemetry-aggregator",
        # Новая group читает с начала.
        "auto.offset.reset": "earliest",
        # Commit после обработки.
        "enable.auto.commit": False,
    }
)

# Producer config.
producer = Producer(
    {
        # Kafka broker.
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        # Подтверждение от leader.
        "acks": "1",
    }
)

consumer.subscribe(["telemetry"])
counts = defaultdict(int)
window_start = int(time.time() // 60 * 60)

while True:
    msg = consumer.poll(1.0)
    current_window = int(time.time() // 60 * 60)

    if current_window != window_start:
        for device_id, count in counts.items():
            aggregate = {
                "window_start": window_start,
                "device_id": device_id,
                "events": count,
            }
            producer.produce(
                "telemetry-1m",
                key=device_id,
                value=json.dumps(aggregate),
            )
            print(aggregate)
        producer.flush()
        counts.clear()
        window_start = current_window

    if msg is None:
        continue
    if msg.error():
        print(f"consumer error: {msg.error()}")
        continue

    event = json.loads(msg.value().decode("utf-8"))
    counts[event["device_id"]] += 1
    consumer.commit(message=msg)
```

### 4. Запуск

Терминал 1:

```bash
cd "$PY_APP_DIR"
source .venv/bin/activate
python telemetry_generator.py
```

Терминал 2:

```bash
cd "$PY_APP_DIR"
source .venv/bin/activate
python telemetry_aggregator.py
```

Терминал 3:

```bash
"$KAFKA_HOME/bin/kafka-console-consumer.sh" \
  --bootstrap-server localhost:9092 \
  --topic telemetry-1m \
  --from-beginning \
  --property print.key=true
```

Ожидаемый вывод:

```text
sensor-1 {"window_start":..., "device_id":"sensor-1", "events":...}
sensor-2 {"window_start":..., "device_id":"sensor-2", "events":...}
```

## Практическая процедура 8. TLS и ACL

### Назначение

Настроить SSL listener Kafka, подключиться к нему Python producer и проверить авторизацию через ACL. После работы должно быть понятно, что TLS отвечает за защищенный транспорт, а ACL отвечает за разрешения на операции с Kafka resources.

### Технический сценарий

Создается локальный CA, сертификат broker, keystore и truststore. Kafka запускается на SSL listener `localhost:9093`. Сначала Python producer подключается по SSL, но получает ошибку авторизации при записи в `secure-events`. Затем через `kafka-acls.sh` добавляется разрешение на `Write` и `Describe`, после чего producer успешно отправляет сообщение.

### Проверяемое состояние

- Broker стартует с SSL listener.
- Client truststore или CA certificate позволяет проверить broker certificate.
- Без ACL запись запрещена при `allow.everyone.if.no.acl.found=false`.
- ACL на topic разрешает конкретную операцию конкретному principal.
- Ошибки TLS и ошибки authorization различаются по симптомам.

### 1. Каталоги сертификатов

```bash
# Каталог TLS-материалов.
sudo mkdir -p /etc/kafka/ssl

# Доступен пользователю kafka.
sudo chown -R kafka:kafka /etc/kafka/ssl
```

### 2. CA и сертификаты в PEM

```bash
# CA private key.
openssl genrsa -out /tmp/ca.key 4096

# CA certificate.
openssl req -x509 -new -nodes \
  -key /tmp/ca.key \
  -sha256 \
  -days 3650 \
  -subj "/CN=kafka-lab-ca" \
  -out /tmp/ca.crt

# Broker key.
openssl genrsa -out /tmp/broker.key 2048

# Broker CSR.
openssl req -new \
  -key /tmp/broker.key \
  -subj "/CN=localhost" \
  -out /tmp/broker.csr

# Broker certificate.
openssl x509 -req \
  -in /tmp/broker.csr \
  -CA /tmp/ca.crt \
  -CAkey /tmp/ca.key \
  -CAcreateserial \
  -out /tmp/broker.crt \
  -days 365 \
  -sha256
```

Ожидаемый вывод:

```text
Certificate request self-signature ok
subject=CN = localhost
```

### 3. Keystore и truststore для broker

```bash
# PKCS12 keystore с broker key и certificate.
openssl pkcs12 -export \
  -in /tmp/broker.crt \
  -inkey /tmp/broker.key \
  -certfile /tmp/ca.crt \
  -name localhost \
  -out /etc/kafka/ssl/broker.keystore.p12 \
  -password pass:changeit

# PKCS12 truststore с CA certificate.
keytool -importcert \
  -noprompt \
  -alias kafka-lab-ca \
  -file /tmp/ca.crt \
  -keystore /etc/kafka/ssl/kafka.truststore.p12 \
  -storetype PKCS12 \
  -storepass changeit

# Копии PEM для Python clients.
sudo cp /tmp/ca.crt /etc/kafka/ssl/ca.crt
sudo cp /tmp/broker.crt /etc/kafka/ssl/broker.crt
sudo cp /tmp/broker.key /etc/kafka/ssl/broker.key
sudo chown -R kafka:kafka /etc/kafka/ssl
```

### 4. SSL broker config

Файл `/opt/kafka/config/kraft/server-ssl.properties`:

```properties
# Идентификатор узла.
node.id=1

# Один процесс broker и controller.
process.roles=broker,controller

# Controller quorum.
controller.quorum.voters=1@localhost:9094

# SSL listener для клиентов и plaintext controller listener.
listeners=SSL://:9093,CONTROLLER://:9094

# SSL endpoint для клиентов.
advertised.listeners=SSL://localhost:9093

# Controller listener.
controller.listener.names=CONTROLLER

# Карта listener security protocols.
listener.security.protocol.map=SSL:SSL,CONTROLLER:PLAINTEXT

# Межброкерный listener.
inter.broker.listener.name=SSL

# Каталог данных SSL broker.
log.dirs=/var/lib/kafka/kraft-ssl-logs

# Keystore broker.
ssl.keystore.location=/etc/kafka/ssl/broker.keystore.p12
ssl.keystore.password=changeit
ssl.key.password=changeit

# Truststore broker.
ssl.truststore.location=/etc/kafka/ssl/kafka.truststore.p12
ssl.truststore.password=changeit

# На первом этапе клиентский сертификат не требуется.
ssl.client.auth=none

# Authorizer для ACL в KRaft.
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer

# Broker principal и admin principal имеют полный доступ.
super.users=User:ANONYMOUS;User:CN=localhost

# При отсутствии ACL доступ запрещен.
allow.everyone.if.no.acl.found=false

# Внутренние topics для single-broker.
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
```

### 5. Admin client properties

Файл `admin-ssl.properties`:

```properties
# Подключение к SSL listener.
security.protocol=SSL

# Truststore с CA broker.
ssl.truststore.location=/etc/kafka/ssl/kafka.truststore.p12
ssl.truststore.password=changeit

# Hostname verification отключена для localhost self-signed lab.
ssl.endpoint.identification.algorithm=
```

### 6. Запуск SSL broker

```bash
# Каталог данных SSL broker.
sudo mkdir -p /var/lib/kafka/kraft-ssl-logs
sudo chown kafka:kafka /var/lib/kafka/kraft-ssl-logs

# Format.
SSL_CLUSTER_ID="$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)"
sudo -u kafka "$KAFKA_HOME/bin/kafka-storage.sh" format \
  -t "$SSL_CLUSTER_ID" \
  -c "$KAFKA_HOME/config/kraft/server-ssl.properties"

# Start.
sudo -u kafka "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_HOME/config/kraft/server-ssl.properties"
```

### 7. Создание secure topic

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9093 \
  --command-config admin-ssl.properties \
  --create \
  --topic secure-events \
  --partitions 1 \
  --replication-factor 1
```

Ожидаемый вывод:

```text
Created topic secure-events.
```

### 8. Python SSL producer

Файл `ssl_producer.py`:

```python
# SSL producer отправляет сообщение в secure-events.
from confluent_kafka import Producer

# Kafka producer SSL config.
producer = Producer(
    {
        # SSL endpoint Kafka.
        "bootstrap.servers": "localhost:9093",
        # SSL transport.
        "security.protocol": "SSL",
        # CA certificate для проверки broker.
        "ssl.ca.location": "/etc/kafka/ssl/ca.crt",
        # Отключение hostname verification для lab.
        "ssl.endpoint.identification.algorithm": "none",
    }
)

producer.produce("secure-events", key="k1", value="ssl-message")
producer.flush()
print("sent")
```

Запуск:

```bash
python ssl_producer.py
```

Ожидаемый результат при отсутствии ACL:

```text
KafkaError{code=TOPIC_AUTHORIZATION_FAILED,...}
```

### 9. ACL для anonymous principal

```bash
# Разрешение записи в topic.
"$KAFKA_HOME/bin/kafka-acls.sh" \
  --bootstrap-server localhost:9093 \
  --command-config admin-ssl.properties \
  --add \
  --allow-principal "User:ANONYMOUS" \
  --operation Write \
  --operation Describe \
  --topic secure-events
```

Ожидаемый вывод:

```text
Adding ACLs for resource `ResourcePattern(resourceType=TOPIC, name=secure-events, patternType=LITERAL)`:
```

Повторный запуск:

```bash
python ssl_producer.py
```

Ожидаемый вывод:

```text
sent
```

## Практическая процедура 9. Мониторинг Kafka

### Назначение

Подключить Prometheus-совместимые метрики Kafka через JMX Exporter и проверить consumer lag штатными Kafka-инструментами. После работы должен быть понятен минимальный набор метрик для контроля broker и consumer groups.

### Технический сценарий

Kafka broker запускается с Java agent JMX Exporter, который публикует JVM/Kafka metrics на HTTP endpoint. Prometheus получает scrape config для JMX Exporter и Kafka Exporter. Consumer lag проверяется через `kafka-consumer-groups.sh`, чтобы связать метрики мониторинга с фактическим состоянием consumer groups.

### Проверяемое состояние

- JMX Exporter endpoint `/metrics` возвращает Prometheus metrics.
- Prometheus scrape config содержит targets Kafka.
- Consumer group lag вычисляется как `LOG-END-OFFSET - CURRENT-OFFSET`.
- Рост lag означает, что consumer group обрабатывает медленнее входящего потока.
- Для диагностики replication важны under-replicated partitions и offline partitions.

### 1. JMX Exporter config

Файл `/opt/jmx-exporter/kafka-broker.yml`:

```yaml
# Lowercase metric names for Prometheus.
lowercaseOutputName: true

# Lowercase label names.
lowercaseOutputLabelNames: true

# Rules select Kafka MBeans.
rules:
  # Broker topic metrics.
  - pattern: kafka.server<type=BrokerTopicMetrics, name=(.+)><>Count
    name: kafka_server_brokertopicmetrics_$1_total
    type: COUNTER

  # Replica manager gauges.
  - pattern: kafka.server<type=ReplicaManager, name=(.+)><>Value
    name: kafka_server_replicamanager_$1
    type: GAUGE
```

### 2. Запуск broker с JMX Exporter

```bash
# Java agent JMX Exporter.
export KAFKA_OPTS="-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=7071:/opt/jmx-exporter/kafka-broker.yml"

# Запуск Kafka.
sudo -u kafka "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_HOME/config/kraft/server.properties"
```

Проверка:

```bash
# Проверка Prometheus metrics endpoint.
curl -s http://localhost:7071/metrics | head
```

Ожидаемый вывод:

```text
# HELP ...
# TYPE ...
```

### 3. Prometheus config

Файл `/etc/prometheus/prometheus.yml`:

```yaml
global:
  # Интервал опроса targets.
  scrape_interval: 15s

scrape_configs:
  # JMX Exporter Kafka broker.
  - job_name: kafka-jmx
    static_configs:
      - targets:
          - localhost:7071

  # Kafka Exporter, если установлен отдельно.
  - job_name: kafka-exporter
    static_configs:
      - targets:
          - localhost:9308
```

### 4. Проверка consumer lag

```bash
# Consumer group lag через штатную утилиту Kafka.
"$KAFKA_HOME/bin/kafka-consumer-groups.sh" \
  --bootstrap-server localhost:9092 \
  --describe \
  --all-groups
```

Ожидаемый вывод:

```text
GROUP  TOPIC  PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
```

Пояснение:

```text
LAG = LOG-END-OFFSET - CURRENT-OFFSET.
Рост LAG означает, что consumer group читает медленнее, чем producer пишет.
```

## Практическая процедура 10. Финальный сценарий: обработка заказов на Python

### Назначение

Собрать прикладной event-driven сценарий обработки заказов на Python: HTTP API публикует событие заказа, несколько независимых consumer groups выполняют разные бизнес-действия и публикуют результаты в отдельные topics. После работы должен быть готов цельный пример асинхронного взаимодействия сервисов через Kafka.

### Технический сценарий

FastAPI service `orders_api.py` принимает HTTP-запрос на создание заказа и пишет событие в `final-orders`. `billing_worker.py` читает тот же topic в group `billing`, создает событие списания и пишет его в `final-billing-events`. `shipping_worker.py` читает `final-orders` в group `shipping`, создает событие доставки и пишет его в `final-shipping-events`. Каждая group имеет собственные offsets и не блокирует другие группы.

### Проверяемое состояние

- HTTP API отделен от фоновой обработки через Kafka.
- Одно событие заказа независимо читают несколько consumer groups.
- Billing и shipping имеют разные offsets и разные output topics.
- Manual commit выполняется после публикации результата.
- Повторная обработка должна учитываться через `order_id` или `event_id`.
- `kafka-consumer-groups.sh` выводит состояние каждой group отдельно.

### 1. Topics

```bash
# Основной поток заказов.
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --create \
  --topic final-orders \
  --partitions 3 \
  --replication-factor 1

# События billing.
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --create \
  --topic final-billing-events \
  --partitions 3 \
  --replication-factor 1

# События shipping.
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --create \
  --topic final-shipping-events \
  --partitions 3 \
  --replication-factor 1
```

### 2. Orders API

Файл `orders_api.py`:

```python
# FastAPI service принимает HTTP request и публикует order event.
import json
import os
import time
from uuid import uuid4

from confluent_kafka import Producer
from dotenv import load_dotenv
from fastapi import FastAPI

# Загрузка .env.
load_dotenv()

# Kafka producer config.
producer = Producer(
    {
        # Kafka broker.
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        # Идемпотентная запись заказов.
        "enable.idempotence": True,
        # Подтверждение записи всеми ISR.
        "acks": "all",
    }
)

app = FastAPI()


@app.post("/orders")
def create_order(customer_id: str, amount: float):
    # order_id используется как id события.
    order_id = str(uuid4())
    event = {
        "order_id": order_id,
        "customer_id": customer_id,
        "amount": amount,
        "status": "created",
        "created_at": time.time(),
    }
    producer.produce(
        "final-orders",
        key=customer_id,
        value=json.dumps(event),
    )
    producer.flush()
    return {"order_id": order_id, "status": "created"}
```

### 3. Billing consumer

Файл `billing_worker.py`:

```python
# Billing service читает final-orders и пишет final-billing-events.
import json
import os

from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

# Загрузка .env.
load_dotenv()

# Consumer config.
consumer = Consumer(
    {
        # Kafka broker.
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        # Отдельная group billing.
        "group.id": "billing",
        # Читать с начала для нового стенда.
        "auto.offset.reset": "earliest",
        # Ручной commit после публикации результата.
        "enable.auto.commit": False,
    }
)

# Producer config.
producer = Producer(
    {
        # Kafka broker.
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        # Подтверждение записи.
        "acks": "all",
    }
)

consumer.subscribe(["final-orders"])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"consumer error: {msg.error()}")
        continue

    order = json.loads(msg.value().decode("utf-8"))
    event = {
        "order_id": order["order_id"],
        "billing_status": "charged",
        "amount": order["amount"],
    }
    producer.produce(
        "final-billing-events",
        key=order["customer_id"],
        value=json.dumps(event),
    )
    producer.flush()
    consumer.commit(message=msg)
    print(f"billing charged order_id={order['order_id']}")
```

### 4. Shipping consumer

Файл `shipping_worker.py`:

```python
# Shipping service читает final-orders и пишет final-shipping-events.
import json
import os

from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

# Загрузка .env.
load_dotenv()

# Consumer config.
consumer = Consumer(
    {
        # Kafka broker.
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        # Отдельная group shipping.
        "group.id": "shipping",
        # Читать с начала для нового стенда.
        "auto.offset.reset": "earliest",
        # Ручной commit после публикации результата.
        "enable.auto.commit": False,
    }
)

# Producer config.
producer = Producer(
    {
        # Kafka broker.
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        # Подтверждение записи.
        "acks": "all",
    }
)

consumer.subscribe(["final-orders"])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"consumer error: {msg.error()}")
        continue

    order = json.loads(msg.value().decode("utf-8"))
    event = {
        "order_id": order["order_id"],
        "shipping_status": "created",
        "customer_id": order["customer_id"],
    }
    producer.produce(
        "final-shipping-events",
        key=order["customer_id"],
        value=json.dumps(event),
    )
    producer.flush()
    consumer.commit(message=msg)
    print(f"shipping created order_id={order['order_id']}")
```

### 5. Запуск сценария

Терминал 1:

```bash
cd "$PY_APP_DIR"
source .venv/bin/activate
uvicorn orders_api:app --host 0.0.0.0 --port 8000
```

Терминал 2:

```bash
cd "$PY_APP_DIR"
source .venv/bin/activate
python billing_worker.py
```

Терминал 3:

```bash
cd "$PY_APP_DIR"
source .venv/bin/activate
python shipping_worker.py
```

Терминал 4:

```bash
# Создание заказа через HTTP.
curl -X POST "http://localhost:8000/orders?customer_id=customer-1&amount=1500"
```

Ожидаемый вывод API:

```json
{"order_id":"...","status":"created"}
```

Ожидаемый вывод billing:

```text
billing charged order_id=...
```

Ожидаемый вывод shipping:

```text
shipping created order_id=...
```

### 6. Проверка topics

```bash
# Billing events.
"$KAFKA_HOME/bin/kafka-console-consumer.sh" \
  --bootstrap-server localhost:9092 \
  --topic final-billing-events \
  --from-beginning \
  --timeout-ms 10000

# Shipping events.
"$KAFKA_HOME/bin/kafka-console-consumer.sh" \
  --bootstrap-server localhost:9092 \
  --topic final-shipping-events \
  --from-beginning \
  --timeout-ms 10000
```

Ожидаемый вывод:

```text
{"order_id":"...","billing_status":"charged","amount":1500.0}
{"order_id":"...","shipping_status":"created","customer_id":"customer-1"}
```

### 7. Проверка независимых consumer groups

```bash
"$KAFKA_HOME/bin/kafka-consumer-groups.sh" \
  --bootstrap-server localhost:9092 \
  --describe \
  --group billing

"$KAFKA_HOME/bin/kafka-consumer-groups.sh" \
  --bootstrap-server localhost:9092 \
  --describe \
  --group shipping
```

Ожидаемый вывод:

```text
GROUP     TOPIC         PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
billing   final-orders  ...        ...             ...             0
shipping  final-orders  ...        ...             ...             0
```
