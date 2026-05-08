# Руководство по стенду Apache Kafka

Файл описывает техническую последовательность настройки и проверки Apache Kafka. Команды, конфиги и приложения привязаны к Rocky Linux, Kafka в KRaft mode и Python-коду для внешних подключений. Подробный разбор параметров конфигурации находится в [04_Справочник_конфигураций.md](04_Справочник_конфигураций.md), режимы работы и прикладные pipeline - в [05_Режимы_работы_и_сценарии.md](05_Режимы_работы_и_сценарии.md).

Последовательность технических блоков:

- сначала запускается одиночный broker, чтобы отделить базовые сущности Kafka от распределенной сложности;
- затем добавляются partitions, keys, offsets и consumer groups;
- после этого вводятся Python producer/consumer как основной способ подключения приложений;
- multi-broker cluster раскрывает replication, ISR, leader election и влияние отказов;
- storage-блок связывает Kafka topics с файлами на диске, retention и compaction;
- внешние интеграции выполняются через Python и PostgreSQL, Kafka Connect разбирается как инфраструктурная альтернатива;
- безопасность разделяется на transport protection через TLS и authorization через ACL;
- мониторинг связывает состояние Kafka с метриками и consumer lag;
- финальные сценарии собирают несколько компонентов в event-driven pipeline.
## 1. Базовый стенд

### Теория

Стенд разделяет инфраструктуру Kafka и прикладные клиенты. Kafka broker запускается как JVM-сервис, хранит данные на диске и принимает сетевые запросы. Python-приложения выступают внешними producer/consumer-клиентами и подключаются к Kafka как реальные сервисы.

### Техническая модель

Стенд должен давать повторяемую среду для всех компонентов. Kafka требует JVM, поэтому Java 17 устанавливается до Kafka. Python используется для приложений: producer, consumer, HTTP API, PostgreSQL source/sink, stream-like обработка. PostgreSQL нужен как внешнее состояние для at-least-once обработки с идемпотентной записью.

### Контрольные положения

- Kafka - инфраструктурный сервис, приложения подключаются к нему по network protocol.
- Java нужна broker-процессу и штатным Kafka CLI.
- Python-приложения подключаются к Kafka как внешние сервисы.
- Каталог `/opt/kafka` содержит бинарные файлы и конфиги, `/var/lib/kafka` содержит данные, потеря которых означает потерю partitions без репликации.
- Все команды диагностики должны различать три уровня: процесс запущен, порт слушается, Kafka protocol отвечает.

Операционная система:

```text
Rocky Linux 9
Java 17
Python 3.11+
Apache Kafka в KRaft mode
PostgreSQL 16 для внешних подключений
Prometheus и Grafana для мониторинга
```

Каталоги:

```text
/opt/kafka                         # Kafka installation
/var/lib/kafka                     # Kafka data
/var/log/kafka                     # Kafka logs
/home/student/kafka-course-python  # Python-приложения
```

Пакеты:

```bash
# Java для Kafka.
sudo dnf install -y java-17-openjdk java-17-openjdk-devel

# Python и инструменты сборки пакетов.
sudo dnf install -y python3 python3-pip python3-devel gcc

# Утилиты для диагностики.
sudo dnf install -y curl jq tar net-tools lsof
```

Проверка:

```bash
java -version
python3 --version
curl --version
```

Ожидаемые признаки:

```text
Java major version: 17
Python major version: 3
curl установлен
```
## 2. Единый Python workspace

### Теория

Kafka-клиент является частью приложения. Producer и consumer не запускаются внутри Kafka broker: они подключаются к нему по network protocol. Единый Python workspace фиксирует версии библиотек, единый `.env` и одинаковую модель подключения к standalone broker или multi-broker cluster.

### Техническая модель

Единое виртуальное окружение исключает расхождение версий библиотек между приложениями. `confluent-kafka` использует `librdkafka` и дает production-поведение Kafka client: delivery callbacks, consumer groups, manual commit, SSL и SASL options. `psycopg` используется для PostgreSQL, `FastAPI` и `uvicorn` - для HTTP-сервиса, который публикует события в Kafka.

### Контрольные положения

- Producer и consumer являются обычными приложениями, Kafka не запускает их сама.
- `.env` отделяет параметры подключения от кода.
- Один и тот же код можно переключить со standalone broker на cluster заменой `bootstrap.servers`.
- Внешняя система, например PostgreSQL, создает проблему согласования offset Kafka и состояния БД; поэтому commit offset должен выполняться после успешной записи в БД.

```bash
# Каталог для Python-приложений.
mkdir -p /home/student/kafka-course-python
cd /home/student/kafka-course-python

# Виртуальное окружение.
python3 -m venv .venv

# Активация окружения.
source .venv/bin/activate

# Kafka client, PostgreSQL client, HTTP framework, Prometheus metrics.
pip install confluent-kafka psycopg[binary] python-dotenv fastapi uvicorn prometheus-client
```

Файл `/home/student/kafka-course-python/.env`:

```dotenv
# KAFKA_BOOTSTRAP_SERVERS
# Варианты: один host:port или несколько через запятую.
# Влияние: используется Python producer/consumer для подключения к standalone broker; неверный адрес даст connection timeout/refused.
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# KAFKA_CLUSTER_BOOTSTRAP_SERVERS
# Варианты: список bootstrap brokers, например localhost:19092,localhost:29092,localhost:39092.
# Влияние: позволяет Python-приложению стартовать, даже если один bootstrap broker недоступен; после metadata client подключается к leaders partitions.
KAFKA_CLUSTER_BOOTSTRAP_SERVERS=localhost:19092,localhost:29092,localhost:39092

# ORDERS_TOPIC
# Варианты: имя существующего topic.
# Влияние: определяет поток заказов; если topic отсутствует при auto.create.topics.enable=false, producer получит ошибку topic metadata.
ORDERS_TOPIC=orders

# APP_LOGS_TOPIC
# Варианты: имя topic для логовых событий.
# Влияние: отделяет технические логи от бизнес-событий; retention и ACL обычно отличаются от orders topics.
APP_LOGS_TOPIC=app-logs

# TELEMETRY_TOPIC
# Варианты: имя topic для IoT/telemetry событий.
# Влияние: key обычно device_id; количество partitions задает параллелизм генерации и обработки телеметрии.
TELEMETRY_TOPIC=telemetry

# POSTGRES_DSN
# Варианты: postgresql://user:password@host:port/database.
# Влияние: используется psycopg; ошибка в user/password/database приведет к authentication failed или database does not exist.
POSTGRES_DSN=postgresql://kafka:kafka@localhost:5432/kafka_lab
```

## 3. Standalone Kafka

### Теория

Kafka рассматривается как распределенный commit log и платформа event streaming. В standalone KRaft один процесс выполняет роли `broker` и `controller`: broker хранит partitions и обслуживает clients, controller управляет metadata. Сообщение после чтения не удаляется, а consumer фиксирует только позицию чтения.

Базовая схема: приложение-источник записывает событие в Kafka, Kafka сохраняет событие на сервере, приложение-получатель читает событие позже. Kafka не является функцией внутри приложения; это отдельный серверный сервис между приложениями.

Первое употребление терминов:

- `producer` - приложение, которое отправляет событие в Kafka;
- `broker` - сервер Kafka, который принимает, хранит и отдает события;
- `cluster` - группа brokers, работающих как одна Kafka-система;
- `topic` - именованный поток событий, например `orders`;
- `partition` - часть topic, отдельный упорядоченный журнал;
- `offset` - номер сообщения внутри partition;
- `consumer` - приложение, которое читает события из Kafka;
- `consumer group` - группа consumers, которые совместно читают partitions topic;
- `replica` - копия partition на broker;
- `leader` - главная replica, через которую идут чтение и запись;
- `follower` - replica, копирующая данные с leader;
- `ISR` - replicas, которые не отстают от leader и считаются синхронными.

Минимальная схема для объяснения:

```text
producer -> broker -> topic -> partition -> offset -> consumer
```

Затем схема расширяется до отказоустойчивости:

```text
topic partition
  leader replica -> broker 1
  follower replica -> broker 2
  follower replica -> broker 3
```

Kafka объясняется как распределенный commit log, а не как классическая очередь. Сообщение после чтения не исчезает из Kafka: оно остается в partition до retention или compaction. Consumer фиксирует только свою позицию чтения. Это сразу объясняет, почему один topic могут читать несколько независимых приложений и почему replay возможен без повторной отправки producer.

Event streaming раскрывается через различие команды и события. Команда просит выполнить действие, событие фиксирует факт. `CreateOrder` - команда, `OrderCreated` - событие. Kafka хранит поток фактов, поэтому downstream-сервисы реагируют на уже произошедшие изменения и не блокируют producer.

Standalone-режим не дает отказоустойчивость. Один процесс содержит data plane и metadata plane. При его остановке недоступны и данные, и управление metadata.

Поток события в минимальной конфигурации:

```text
producer process
  -> TCP connection
  -> Kafka broker listener
  -> topic partition log
  -> consumer group fetch
  -> consumer side effect
  -> offset commit
```

Сообщение в Kafka является record в partition log. Topic группирует records по смыслу, partition дает порядок и параллелизм, offset фиксирует позицию, consumer group хранит прогресс чтения. Эти сущности образуют базовую модель Kafka.

### Техническая модель

Standalone Kafka в KRaft mode содержит минимальный состав Kafka без ZooKeeper. Один процесс содержит две логические роли. Роль `broker` принимает producer/consumer/admin requests и хранит partitions. Роль `controller` ведет metadata: список brokers, topics, partitions, leaders, ISR. В single-node режиме quorum состоит из одного controller voter, поэтому отказ процесса полностью останавливает и data plane, и metadata plane.

### Контрольные положения

- KRaft заменяет ZooKeeper и хранит metadata внутри Kafka quorum.
- `kafka-storage.sh format` обязателен перед первым запуском KRaft, потому что каталог данных должен получить `cluster.id`.
- `listeners` определяет, где процесс слушает подключения, `advertised.listeners` определяет, какой адрес увидит клиент в metadata.
- `replication.factor=1` означает отсутствие отказоустойчивости данных.
- Проверка Kafka начинается не с topic, а с protocol-level ответа `kafka-broker-api-versions.sh`.

### Последовательность настройки и проверки

Создается базовый broker, проверяется TCP/API доступность, создается первый topic и фиксируется связь между конфигом, запущенным процессом и состоянием metadata.

Техническое состояние к началу:

```text
Kafka распакована в /opt/kafka
Каталог /var/lib/kafka/kraft-combined-logs пустой или отформатирован под один cluster.id
Порт 9092 свободен
Порт 9093 свободен
```

Конфиг `/opt/kafka/config/kraft/server.properties`:

```properties
# node.id
# Варианты: целое число, уникальное внутри KRaft cluster.
# Влияние: broker/controller регистрируется под этим id; повтор id у разных nodes ломает quorum и регистрацию broker.
node.id=1

# process.roles
# Варианты: broker, controller, broker,controller.
# Влияние: broker хранит partitions и обслуживает clients; controller управляет metadata; broker,controller уменьшает число процессов.
process.roles=broker,controller

# controller.quorum.voters
# Варианты: список nodeId@host:port через запятую.
# Влияние: определяет KRaft quorum; один voter не дает отказоустойчивости, три voters выдерживают отказ одного controller.
controller.quorum.voters=1@localhost:9093

# listeners
# Варианты: listenerName://host:port; пустой host означает слушать все интерфейсы.
# Влияние: задает локальные sockets Kafka; конфликт портов или неверный listener остановит запуск broker.
listeners=PLAINTEXT://:9092,CONTROLLER://:9093

# advertised.listeners
# Варианты: listenerName://host:port, доступный clients.
# Влияние: адрес возвращается clients в metadata; localhost подходит только локальным clients.
advertised.listeners=PLAINTEXT://localhost:9092

# controller.listener.names
# Варианты: имя listener из listeners, обычно CONTROLLER.
# Влияние: используется для metadata quorum; producer/consumer не подключаются к этому listener.
controller.listener.names=CONTROLLER

# listener.security.protocol.map
# Варианты: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL для каждого listener.
# Влияние: определяет security protocol; SSL/SASL_SSL требуют дополнительных TLS/SASL параметров.
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

# inter.broker.listener.name
# Варианты: имя listener из listener.security.protocol.map.
# Влияние: используется для replication и broker-to-broker traffic; неверное значение приводит к проблемам ISR/replication.
inter.broker.listener.name=PLAINTEXT

# log.dirs
# Варианты: один каталог или несколько каталогов через запятую.
# Влияние: хранит partition logs, indexes и KRaft metadata log; потеря каталога при RF=1 означает потерю данных.
log.dirs=/var/lib/kafka/kraft-combined-logs

# num.partitions
# Варианты: целое число от 1.
# Влияние: partitions по умолчанию; больше partitions дают параллелизм, но увеличивают metadata и число файлов.
num.partitions=1

# offsets.topic.replication.factor
# Варианты: 1 для standalone, 3 для production-кластера.
# Влияние: replication factor __consumer_offsets; значение выше числа brokers ломает создание offsets topic.
offsets.topic.replication.factor=1

# transaction.state.log.replication.factor
# Варианты: 1 для standalone, 3 для production.
# Влияние: replication factor transaction state; нужен transactional producers и exactly-once внутри Kafka.
transaction.state.log.replication.factor=1

# transaction.state.log.min.isr
# Варианты: 1 для standalone, обычно 2 при RF=3.
# Влияние: минимум ISR для transaction state; при меньшем ISR transactional writes отклоняются.
transaction.state.log.min.isr=1
```

Запуск:

```bash
# Генерация cluster.id.
KAFKA_CLUSTER_ID="$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)"

# Форматирование хранилища.
sudo -u kafka "$KAFKA_HOME/bin/kafka-storage.sh" format \
  -t "$KAFKA_CLUSTER_ID" \
  -c "$KAFKA_HOME/config/kraft/server.properties"

# Запуск foreground-процесса.
sudo -u kafka "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_HOME/config/kraft/server.properties"
```

Проверка из второго терминала:

```bash
"$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server localhost:9092
```

Состояние после блока:

```text
Broker id=1 отвечает на localhost:9092
Topic test-events создается и описывается
Console producer и consumer обмениваются сообщениями
```

Типовые ошибки:

```text
Cluster ID mismatch
  Причина: каталог log.dirs уже форматировался другим cluster.id.
  Проверка: cat /var/lib/kafka/kraft-combined-logs/meta.properties.
  Исправление: остановить broker, очистить каталог данных, повторить format.

Address already in use
  Причина: порт 9092 или 9093 занят.
  Проверка: sudo ss -ltnp | grep -E '9092|9093'.
  Исправление: остановить лишний процесс или сменить порты listeners.

Connection refused
  Причина: broker не запущен или advertised.listeners указывает неверный адрес.
  Проверка: lsof -i :9092 и broker logs.
```
## 4. Topics, partitions, offsets

### Теория

Topic является логическим потоком событий, partition является физическим append-only журналом, offset является позицией record внутри конкретной partition. Key управляет маршрутизацией records по partitions и влияет на порядок событий по бизнес-сущности. Replication factor отвечает за копии данных, partitions отвечают за параллелизм.

Topic - логическое имя потока, partition - физический журнал. Offset уникален только в пределах partition, поэтому координата сообщения всегда состоит из topic, partition и offset. Offset не является бизнес-идентификатором и не заменяет `event_id`.

Key используется для маршрутизации и группировки. Если key равен `customer_id`, события одного клиента обычно остаются в одной partition и сохраняют порядок внутри этой partition. Если key отсутствует, producer распределяет records по partitions ради баланса нагрузки, но порядок по бизнес-сущности не гарантируется.

Количество partitions задает верхнюю границу активного параллелизма одной consumer group. Topic с 3 partitions не может активно читаться 10 consumers внутри одной group: 3 consumers получат partitions, остальные будут простаивать. При этом 10 разных consumer groups могут независимо читать все 3 partitions.

Replication factor и partitions нельзя смешивать в одно понятие. Partitions дают параллелизм, replication factor дает копии данных. Topic с 12 partitions и replication factor 3 имеет 36 partition replicas на диске кластера.

Выбор key определяет компромисс между порядком и равномерностью. Key по `order_id` сохраняет порядок заказа, key по `customer_id` сохраняет порядок событий клиента, отсутствие key дает распределение без порядка по сущности. Изменение числа partitions меняет hash mapping и влияет на порядок новых событий с тем же key.

```text
same key -> same partition -> ordered sequence for that key
no key   -> balanced writes  -> no entity-level ordering
```

### Техническая модель

Topic - логическая категория событий, но фактическое хранение и параллелизм определяются partitions. Partition является упорядоченным append-only журналом. Offset имеет смысл только внутри конкретной partition. Consumer group хранит offsets отдельно от topic, поэтому один topic может быть источником для нескольких независимых приложений.

### Контрольные положения

- Порядок сообщений гарантируется только внутри одной partition.
- Key не является уникальным идентификатором; key используется partitioner для выбора partition и для compaction.
- Увеличение partitions меняет распределение новых key и может нарушить прежнее соответствие key -> partition.
- Replication factor отвечает за число копий, partitions отвечают за параллелизм.
- `Leader`, `Replicas`, `Isr` в `--describe` отражают фактическое состояние размещения данных.

### Последовательность настройки и проверки

Создаем topic `orders`, отправляем key-value сообщения, читаем их с печатью partition/offset и проверяем committed offsets group. На этом блоке важно связать вывод CLI с моделью хранения: каждый offset относится к одной partition, а consumer group хранит прогресс отдельно.

Демонстрационные commands:

```bash
# Создание topic с тремя partitions.
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 1

# Описание metadata.
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders

# Увеличение числа partitions.
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic orders \
  --partitions 6
```

Проверка распределения по partitions через Python producer:

```python
# file: keyed_orders_producer.py
import json
import os

from confluent_kafka import Producer
from dotenv import load_dotenv

# Загрузка .env.
load_dotenv()

# Producer config.
producer = Producer(
    {
        # Kafka broker.
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        # Доставка подтверждается leader replica.
        "acks": "1",
    }
)


def report(error, message):
    # Печать partition и offset для проверки маршрутизации по key.
    if error:
        print(f"error={error}")
    else:
        print(
            f"key={message.key().decode()} "
            f"partition={message.partition()} "
            f"offset={message.offset()}"
        )


topic = os.environ["ORDERS_TOPIC"]

for i in range(1, 11):
    customer_id = f"customer-{i % 3}"
    event = {"order_id": i, "customer_id": customer_id, "amount": i * 100}
    producer.produce(
        topic=topic,
        key=customer_id,
        value=json.dumps(event),
        callback=report,
    )

producer.flush()
```

Ожидаемый признак:

```text
Одинаковые key попадают в одну partition при неизменном количестве partitions.
Offset растет независимо в каждой partition.
```
## 5. Producers, consumers, consumer groups

### Теория

Producer сериализует record, выбирает partition, формирует batch и отправляет его leader broker. Consumer читает partitions, участвует в consumer group и фиксирует offsets. Delivery semantics определяется тем, когда producer получает ack и когда consumer фиксирует offset относительно бизнес-обработки.

Producer не отправляет каждое сообщение сразу как отдельную операцию. Kafka client накапливает records в batches по partition, применяет compression и отправляет Produce request leader broker. Поэтому `batch.size`, `linger.ms` и `compression.type` влияют на throughput и latency.

`acks=0`, `acks=1` и `acks=all` нужно объяснять через момент подтверждения записи. `acks=0` дает минимальную задержку и максимальный риск потери. `acks=1` подтверждает запись leader. `acks=all` подтверждает запись ISR replicas и раскрывается вместе с `min.insync.replicas`.

Consumer group - механизм горизонтального масштабирования чтения. Kafka распределяет partitions, а не отдельные messages. Rebalance происходит при изменении состава group или partitions. Долгая обработка без `poll()` приводит к исключению consumer из group по `max.poll.interval.ms`.

Delivery semantics объясняется через порядок commit offset и side effect. Commit до обработки дает at-most-once. Commit после обработки дает at-least-once и требует идемпотентности. Exactly-once внутри Kafka достигается transactions, но внешняя БД требует собственных idempotency keys или outbox/inbox.

Serialization связано с тем, что Kafka хранит bytes. JSON удобен для чтения человеком и отладки, но schema evolution остается ответственностью приложений. Для production-контрактов часто применяются Avro или Protobuf со Schema Registry.

Consumer loop связывается с group membership. `poll()` получает данные и поддерживает активность consumer. Если приложение слишком долго обрабатывает batch и не вызывает `poll()`, group coordinator запускает rebalance. Поэтому размер batch, время обработки и `max.poll.interval.ms` должны соответствовать друг другу.

```text
poll -> process -> external write -> commit
             |
             +-- failure before commit = повторная обработка
```

Producer reliability описывается связкой `acks`, `retries`, `enable.idempotence`, `delivery.timeout.ms`. Consumer reliability описывается связкой `enable.auto.commit`, ручной commit, идемпотентный side effect, обработка повторов.

### Техническая модель

Producer выбирает partition, сериализует сообщение и отправляет record batch broker-leader. Consumer состоит в group, получает назначение partitions от group coordinator и периодически фиксирует offsets. Балансировка возникает не между messages, а между partitions: одна partition внутри одной group в один момент назначается только одному active consumer.

### Контрольные положения

- `acks` управляет моментом, когда producer считает запись успешной.
- `enable.idempotence=true` защищает от дублей при retry на стороне producer.
- `enable.auto.commit=false` переносит ответственность за offset commit в приложение.
- At-least-once означает, что повторная обработка возможна и должна быть безопасной.
- Если consumers больше, чем partitions, лишние consumers не получают partitions.
- Rebalance временно приостанавливает чтение group и перераспределяет partitions.

### Последовательность настройки и проверки

Запускаем Python producer с delivery callback и Python consumer с ручным commit. Затем запускаем два экземпляра consumer и показываем перераспределение partitions. Этот блок переводит Kafka из набора CLI-команд в модель прикладного кода.

Python consumer с ручным commit:

```python
# file: manual_commit_consumer.py
import json
import os
import time

from confluent_kafka import Consumer
from dotenv import load_dotenv

# Загрузка .env.
load_dotenv()

# Consumer config.
consumer = Consumer(
    {
        # Kafka broker.
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        # Consumer group для проверки rebalance.
        "group.id": "orders-workers",
        # Начало чтения для новой group.
        "auto.offset.reset": "earliest",
        # Ручной commit после обработки.
        "enable.auto.commit": False,
        # Ограничение числа сообщений за poll.
        "max.poll.interval.ms": 300000,
    }
)

consumer.subscribe([os.environ["ORDERS_TOPIC"]])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"error={msg.error()}")
            continue

        event = json.loads(msg.value().decode("utf-8"))
        print(
            f"processed order_id={event['order_id']} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )

        # Имитация обработки.
        time.sleep(0.2)

        # Offset фиксируется после обработки.
        consumer.commit(message=msg)
finally:
    consumer.close()
```

Команды для проверки balancing:

```bash
# Терминал 1.
python manual_commit_consumer.py

# Терминал 2.
python manual_commit_consumer.py

# Терминал 3.
python keyed_orders_producer.py

# Проверка group lag.
"$KAFKA_HOME/bin/kafka-consumer-groups.sh" \
  --bootstrap-server localhost:9092 \
  --describe \
  --group orders-workers
```

Ожидаемое состояние:

```text
Partitions распределены между двумя consumer instances.
При остановке одного instance partitions переходят второму.
CURRENT-OFFSET растет после commit.
LAG уменьшается до 0 после обработки backlog.
```
## 6. Multi-broker cluster

### Теория

Кластер Kafka распределяет partitions между brokers и создает replicas для отказоустойчивости. Для каждой partition есть leader replica и follower replicas. Controller отслеживает brokers, ISR и выполняет leader election. Надежность записи определяется связкой `replication.factor`, `min.insync.replicas`, `acks=all` и фактическим состоянием ISR.

Кластер Kafka разделяет metadata plane и data plane. Controller управляет metadata: topics, partitions, leader replicas, ISR и broker membership. Producer и consumer после получения metadata работают напрямую с brokers, которые являются leaders нужных partitions.

KRaft controller quorum хранит metadata в собственном журнале Kafka. В combined mode каждый процесс является и broker, и controller. В separated mode controller nodes не обслуживают user topics, а broker nodes не голосуют в controller quorum. Separated mode сложнее, но устойчивее при высокой клиентской нагрузке.

Leader election должен выбираться из ISR, чтобы не потерять acknowledged records. Unclean leader election повышает доступность, но может выбрать out-of-sync replica и потерять данные. Для критичных потоков это обычно недопустимо.

Отказоустойчивость записи определяется связкой `replication.factor`, `min.insync.replicas`, `acks=all` и состоянием ISR. Нельзя оценивать надежность только по replication factor: если ISR уже уменьшился, фактическая защита ниже.

Client routing через metadata:

```text
client -> bootstrap broker -> metadata response
metadata -> leader for each partition
producer/consumer -> direct connection to leader broker
```

Controller не принимает каждое сообщение. Его задача - metadata и leader election. Produce/fetch traffic идет напрямую к leader brokers, поэтому неверный `advertised.listeners` ломает clients даже при запущенном broker.

### Техническая модель

Кластер Kafka отделяет логический topic от конкретного сервера. Partition replicas размещаются на разных brokers. Для каждой partition один broker является leader, остальные followers копируют данные. Controller отслеживает доступность brokers и выбирает нового leader из ISR при отказе текущего leader.

### Контрольные положения

- Все nodes одного KRaft cluster должны быть отформатированы одним `cluster.id`.
- `node.id` должен быть уникальным, а `controller.quorum.voters` одинаковым на всех nodes.
- `replication.factor=3` не гарантирует успешную запись сам по себе; на запись влияет `acks` producer и `min.insync.replicas`.
- ISR - главный индикатор того, какие replicas действительно синхронны.
- При `acks=all` и `min.insync.replicas=2` отказ одного broker допустим, отказ двух brokers обычно останавливает запись.

### Последовательность настройки и проверки

Запускаются три brokers на разных портах, создается replicated topic, анализируются `Leader`, `Replicas`, `Isr`, затем останавливается один broker и проверяется изменение leaders и ISR. Отказоустойчивость определяется состоянием replicas и quorum, а не только количеством процессов.

Порты:

```text
broker 1: PLAINTEXT 19092, CONTROLLER 19093
broker 2: PLAINTEXT 29092, CONTROLLER 29093
broker 3: PLAINTEXT 39092, CONTROLLER 39093
```

Конфиг broker 2 отличается только идентификатором, портами и каталогом:

```properties
# node.id
# Варианты: уникальное целое число; для этого broker используется 2.
# Влияние: должно совпадать с записью 2@localhost:29093 в controller.quorum.voters; дублирование id нарушит работу кластера.
node.id=2

# process.roles
# Варианты: broker, controller, broker,controller.
# Влияние: combined mode позволяет одному процессу хранить user data и участвовать в metadata quorum.
process.roles=broker,controller

# controller.quorum.voters
# Варианты: полный список всех controller nodes в формате nodeId@host:port.
# Влияние: список должен быть одинаковым на broker 1/2/3; при трех voters quorum переживает отказ одного controller.
controller.quorum.voters=1@localhost:19093,2@localhost:29093,3@localhost:39093

# listeners
# Варианты: client listener и controller listener на свободных портах.
# Влияние: на одном сервере порты всех brokers должны различаться; порт 29092 принимает clients, 29093 обслуживает quorum.
listeners=PLAINTEXT://:29092,CONTROLLER://:29093

# advertised.listeners
# Варианты: адрес второго broker, доступный clients.
# Влияние: client получает этот адрес из metadata; если он совпадет с broker 1 или недоступен, client не сможет писать leaders на broker 2.
advertised.listeners=PLAINTEXT://localhost:29092

# controller.listener.names
# Варианты: имя controller listener, обычно CONTROLLER.
# Влияние: отделяет metadata traffic от client produce/fetch traffic.
controller.listener.names=CONTROLLER

# listener.security.protocol.map
# Варианты: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
# Влияние: задает security protocol для listeners; все brokers должны иметь согласованные внутренние протоколы.
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

# inter.broker.listener.name
# Варианты: listener для broker-to-broker traffic.
# Влияние: используется replication fetch; недоступность listener уменьшит ISR.
inter.broker.listener.name=PLAINTEXT

# log.dirs
# Варианты: отдельный каталог для каждого broker.
# Влияние: хранит данные broker 2; один каталог нельзя использовать несколькими brokers.
log.dirs=/var/lib/kafka/broker-2

# offsets.topic.replication.factor
# Варианты: 1..число brokers; для трех brokers используется 3.
# Влияние: защищает committed offsets consumer groups от потери одного broker.
offsets.topic.replication.factor=3

# transaction.state.log.replication.factor
# Варианты: 1..число brokers; для отказоустойчивых транзакций используется 3.
# Влияние: хранит transaction state; значение выше числа brokers не даст transactional subsystem стартовать корректно.
transaction.state.log.replication.factor=3

# transaction.state.log.min.isr
# Варианты: 1..transaction.state.log.replication.factor; обычно 2 при RF=3.
# Влияние: transactional writes отклоняются, если ISR меньше 2.
transaction.state.log.min.isr=2

# num.partitions
# Варианты: целое число от 1.
# Влияние: partitions по умолчанию для новых topics; 3 дает распределение partitions по трем brokers.
num.partitions=3
```

Команды:

```bash
# Один cluster.id для всех трех каталогов.
KAFKA_CLUSTER_ID="$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)"

# Форматирование broker 1.
sudo -u kafka "$KAFKA_HOME/bin/kafka-storage.sh" format \
  -t "$KAFKA_CLUSTER_ID" \
  -c "$KAFKA_HOME/config/kraft/broker-1.properties"

# Форматирование broker 2.
sudo -u kafka "$KAFKA_HOME/bin/kafka-storage.sh" format \
  -t "$KAFKA_CLUSTER_ID" \
  -c "$KAFKA_HOME/config/kraft/broker-2.properties"

# Форматирование broker 3.
sudo -u kafka "$KAFKA_HOME/bin/kafka-storage.sh" format \
  -t "$KAFKA_CLUSTER_ID" \
  -c "$KAFKA_HOME/config/kraft/broker-3.properties"
```

Проверка отказоустойчивости:

```bash
# Создание replicated topic.
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:19092 \
  --create \
  --topic replicated-orders \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2

# Описание replicas и ISR.
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:19092 \
  --describe \
  --topic replicated-orders
```

После остановки одного broker:

```text
Topic остается доступным.
ISR уменьшается с трех replicas до двух.
Новые записи с acks=all успешны, пока доступно не меньше min.insync.replicas.
```
## 7. Storage, retention, compaction

### Теория

Kafka хранит partitions на диске как набор segment files. Retention удаляет старые closed segments по времени или размеру. Compaction сохраняет последнее значение для каждого key и используется для topics состояния. Производительность Kafka связана с последовательным IO, page cache, batch, compression и количеством partitions.

Kafka хранит partition как последовательность segment files. Active segment принимает новые records, closed segments становятся кандидатами для retention и compaction. Старые records могут не удалиться сразу: segment должен закрыться, а cleaner/checker должен выполнить фоновый цикл.

Retention работает на уровне partition log, а не consumer group. Если consumer не успел прочитать records до удаления, Kafka не восстанавливает их из committed offsets. Retention проектируется по максимальному допустимому простою consumers и требованиям replay.

Compaction хранит последнее значение key и подходит для состояния: профиль пользователя, конфигурации, справочники, changelog state store. Tombstone `value=null` обозначает удаление key. Он не удаляется мгновенно, потому что consumers должны успеть увидеть факт удаления при восстановлении состояния.

Производительность Kafka связана с последовательным IO и page cache. Слишком большой JVM heap может ухудшить работу, потому что уменьшит память ОС под cache. Для throughput важны batch, compression, число partitions, network и disk IO. Для latency важны `linger.ms`, `acks`, request queues, GC и скорость consumer processing.

Физическая структура partition:

```text
topic-partition/
  base-offset.log       records
  base-offset.index     offset -> file position
  base-offset.timeindex timestamp -> offset
```

Retention удаляет closed segments. Compaction переписывает closed segments и оставляет последнее значение для key. Active segment остается открытым для записи и не удаляется обычной проверкой retention.

### Техническая модель

Kafka хранит данные как набор segment files внутри каталога partition. Retention удаляет старые closed segments, а не отдельные records. Compaction переписывает log так, чтобы оставить последнее значение для каждого key. Эти механизмы решают разные задачи: retention ограничивает историю, compaction хранит актуальное состояние.

### Контрольные положения

- Partition на диске - это каталог `topic-partition`.
- `.log` содержит records, `.index` связывает offset с позицией в файле, `.timeindex` ускоряет поиск по времени.
- Active segment не удаляется retention-процессом до закрытия.
- `segment.bytes` и `segment.ms` влияют на то, как быстро данные становятся eligible для retention/compaction.
- Compaction требует key; сообщения без key не дают состояния "последнее значение ключа".
- Tombstone record с `value=null` используется для удаления key в compacted topic.

### Последовательность настройки и проверки

Создается topic с коротким retention и compacted topic. Проверяются файлы на диске, отправляются повторяющиеся keys, читается topic до и после compaction. Настройки topic связываются с физическим хранением.

Команды:

```bash
# Topic с коротким retention.
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --create \
  --topic retention-demo \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=60000 \
  --config segment.ms=10000 \
  --config segment.bytes=1048576

# Topic с compaction.
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --create \
  --topic compact-demo \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=compact \
  --config segment.ms=10000 \
  --config min.cleanable.dirty.ratio=0.01
```

Проверка файлов:

```bash
# Каталоги partitions.
sudo find /var/lib/kafka -maxdepth 2 -type d -name 'retention-demo-*' -o -name 'compact-demo-*'

# Segment files.
sudo find /var/lib/kafka -path '*retention-demo-0*' -type f
```

Ожидаемые признаки:

```text
Retention удаляет закрытые segments после истечения retention.ms.
Compaction оставляет последнее значение для каждого key после работы cleaner.
Tombstone value=null удаляет key после delete.retention.ms.
```
## 8. Внешние подключения через Python и Kafka Connect

### Теория

Интеграция Kafka с внешней системой требует управления двумя состояниями: что уже прочитано из source и что уже применено в sink. Python-код выполняет эту механику явно. Kafka Connect решает похожую задачу через workers, connectors, tasks, converters и internal topics.

Интеграция с внешней системой всегда содержит две позиции: позицию чтения из source и позицию чтения из Kafka. Source-приложение должно понимать, какие строки уже отправлены. Sink-приложение должно понимать, какие Kafka records уже применены к внешнему состоянию.

Polling по incrementing id реализует базовую механику, но это не полноценный CDC. Он не фиксирует update существующей строки и delete. Production CDC обычно использует database log: logical replication, Debezium или специализированный connector.

Kafka Connect состоит из worker, connector и task. Worker запускает runtime, connector описывает интеграцию, tasks выполняют работу параллельно. Distributed mode хранит configs, offsets и statuses в Kafka topics, поэтому Connect cluster может перераспределять tasks между workers.

Converters определяют формат records. JSON без schemas совместим с простыми Python consumers. Avro/Protobuf со Schema Registry дают строгие контракты и совместимость версий.

Source и sink имеют разные точки отказа:

```text
source failure before produce  -> строка будет прочитана снова
source failure after produce   -> нужен сохраненный source offset
sink failure before DB commit  -> Kafka offset не фиксируется
sink failure after DB commit   -> Kafka record повторится, DB write должен быть идемпотентным
```

Polling по `id > last_id` не фиксирует update/delete. CDC через database log фиксирует insert/update/delete и сохраняет порядок изменений на уровне журнала базы.

### Техническая модель

Kafka часто стоит между приложением и внешней системой. Внешние подключения на Python явно выполняют прикладную ответственность: чтение из БД, формирование события, публикация в Kafka, чтение из Kafka, запись во внешнюю БД и commit offset после успешной записи.

Kafka Connect является стандартным runtime для connectors. Он снимает часть кода с приложения, но вводит свои режимы работы, внутренние topics, REST API, converters и lifecycle tasks. Python-подход делает механику обработки явной, Connect-подход важен для production-интеграций с готовыми connectors.

### Контрольные положения

- Source-приложение должно иметь понятный способ определить "что уже отправлено".
- Polling по `id > last_id` прост, но не видит update/delete существующих строк.
- Sink-приложение должно быть идемпотентным, потому что Kafka consumer обычно работает at-least-once.
- Offset commit до записи в PostgreSQL может привести к потере события для sink.
- Offset commit после записи в PostgreSQL может привести к повторной записи, поэтому нужен UPSERT или idempotency key.
- Kafka Connect distributed mode хранит configs, offsets и statuses во внутренних Kafka topics.

### Последовательность настройки и проверки

Создаем PostgreSQL schema, запускаем Python source и Python sink, вставляем строку в `orders` и проверяем появление строки в `processed_orders`. Затем связываем этот поток с Kafka Connect: какие части можно заменить connector runtime, какие настройки отвечают за состояние connector.

PostgreSQL schema:

```sql
-- Пользователь для Python-приложений.
CREATE USER kafka WITH PASSWORD 'kafka';

-- База данных для Kafka/PostgreSQL pipeline.
CREATE DATABASE kafka_lab OWNER kafka;

-- Подключение к базе kafka_lab выполняется отдельно.
\c kafka_lab

-- Таблица-источник заказов.
CREATE TABLE orders (
    id BIGSERIAL PRIMARY KEY,
    customer_id TEXT NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Таблица-приемник обработанных заказов.
CREATE TABLE processed_orders (
    id BIGINT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

-- Права пользователя Python-приложений.
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO kafka;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO kafka;
```

Демонстрационный поток:

```text
PostgreSQL orders -> Python producer -> Kafka topic pg_orders -> Python consumer -> PostgreSQL processed_orders
```

Контрольные команды:

```bash
# Запуск producer, который опрашивает таблицу orders.
python pg_orders_producer.py

# Запуск sink consumer.
python pg_orders_sink.py

# Вставка тестовой строки.
psql "postgresql://kafka:kafka@localhost:5432/kafka_lab" \
  -c "INSERT INTO orders(customer_id, amount, status) VALUES ('c-1', 1500.00, 'created');"

# Проверка результата sink.
psql "postgresql://kafka:kafka@localhost:5432/kafka_lab" \
  -c "SELECT * FROM processed_orders ORDER BY id;"
```

Ожидаемый признак:

```text
producer печатает delivered topic=pg_orders partition=N offset=M
sink печатает stored id=...
processed_orders содержит строку из orders
```

Kafka Connect остается инфраструктурной альтернативой:

```properties
# bootstrap.servers
# Варианты: один broker или список brokers через запятую.
# Влияние: Connect worker использует Kafka для internal topics и connector data; недоступность brokers останавливает tasks.
bootstrap.servers=localhost:9092

# group.id
# Варианты: строка, одинаковая для workers одного Connect cluster.
# Влияние: workers с одним group.id распределяют connector tasks; изменение group.id создает новый независимый Connect cluster.
group.id=connect-cluster

# config.storage.topic
# Варианты: имя Kafka topic, обычно compacted.
# Влияние: хранит configs connectors; потеря topic означает потерю конфигураций Connect cluster.
config.storage.topic=connect-configs

# offset.storage.topic
# Варианты: имя Kafka topic.
# Влияние: хранит offsets source connectors; потеря offsets может привести к повторному чтению source или пропуску данных.
offset.storage.topic=connect-offsets

# status.storage.topic
# Варианты: имя Kafka topic.
# Влияние: хранит статусы connectors/tasks; используется REST API для RUNNING/FAILED/PAUSED.
status.storage.topic=connect-status

# config.storage.replication.factor
# Варианты: 1 для single-broker, 3 для production.
# Влияние: RF internal topic configs; значение выше числа brokers не даст topic создаться.
config.storage.replication.factor=1

# offset.storage.replication.factor
# Варианты: 1 для single-broker, 3 для production.
# Влияние: защищает source offsets; низкий RF повышает риск повторной загрузки после отказа.
offset.storage.replication.factor=1

# status.storage.replication.factor
# Варианты: 1 для single-broker, 3 для production.
# Влияние: защищает статусы tasks; влияет на восстановление состояния Connect после отказов.
status.storage.replication.factor=1

# key.converter
# Варианты: JsonConverter, StringConverter, ByteArrayConverter, AvroConverter, ProtobufConverter.
# Влияние: определяет формат key Kafka records; должен быть совместим с downstream consumers.
key.converter=org.apache.kafka.connect.json.JsonConverter

# key.converter.schemas.enable
# Варианты: true, false.
# Влияние: false пишет plain JSON без schema envelope и удобен для Python; true добавляет schema metadata.
key.converter.schemas.enable=false

# value.converter
# Варианты: JsonConverter, StringConverter, ByteArrayConverter, AvroConverter, ProtobufConverter.
# Влияние: определяет формат payload; Avro/Protobuf обычно требуют Schema Registry.
value.converter=org.apache.kafka.connect.json.JsonConverter

# value.converter.schemas.enable
# Варианты: true, false.
# Влияние: false записывает JSON без schema envelope; true нужен некоторым connectors и schema-aware pipelines.
value.converter.schemas.enable=false

# plugin.path
# Варианты: один или несколько каталогов через запятую.
# Влияние: worker ищет connector plugins в этих каталогах; после добавления jars обычно нужен restart worker.
plugin.path=/opt/kafka/plugins

# rest.host.name
# Варианты: hostname или IP для REST API worker.
# Влияние: определяет, где доступно управление connectors; неверный адрес мешает REST calls между users/tools и worker.
rest.host.name=localhost

# rest.port
# Варианты: свободный TCP port, часто 8083.
# Влияние: конфликт порта не даст Connect worker стартовать.
rest.port=8083
```

## 9. Stream processing через Python

### Теория

Stream processing строится как topology: input topic, преобразования, stateful или stateless operations и output topic. Stateful processing требует состояния и правил восстановления после сбоя. Windowing добавляет временные границы и правила обработки поздних событий.

Stream processing строится как topology: source topic, преобразования, stateful operations и sink topic. Stateless operations (`filter`, `map`) не требуют памяти между records. Stateful operations (`count`, `aggregate`, `join`) требуют state store и восстановления состояния после сбоя.

Windowing вводит границы времени. Tumbling windows не пересекаются, hopping windows пересекаются, session windows зависят от паузы между событиями. Поздние события требуют grace period и правил пересчета результата.

Kafka Streams использует changelog topics для восстановления state stores и repartition topics при изменении key. Python processor реализует базовую механику, но не реализует полноценную обработку late events, state recovery и transactional exactly-once.

Stateful topology:

```text
input topic
  -> repartition by key
  -> aggregate/count/join
  -> local state store
  -> changelog topic
  -> output topic
```

Windowing зависит от timestamp. Event time использует время события, processing time использует время обработки, log append time использует время записи broker. Выбор времени влияет на поздние события и корректность агрегатов.

### Техническая модель

Stream processing - это непрерывное чтение input topic, преобразование событий и запись результата в output topic. Stateless операции не требуют хранения состояния между сообщениями. Stateful операции, например count или aggregate, требуют state store или прикладного состояния. Windowing добавляет границы времени и правила обработки поздних событий.

### Контрольные положения

- Stream processor одновременно является consumer и producer.
- Key определяет не только partitioning, но и корректность группировки.
- Aggregation без состояния невозможна: приложение должно хранить промежуточные счетчики.
- Commit input offset после output produce дает at-least-once результат и возможные дубли в output.
- Для production exactly-once stream processing обычно используют Kafka Streams transactions или специализированный stream engine.
- Простая Python-реализация содержит базовую механику, но не заменяет полноценный stateful engine для сложных окон.

### Последовательность настройки и проверки

Генерируются telemetry events, Python processor читает их, считает события по `device_id` в минутном окне и публикует агрегаты. Блок содержит topology `input -> process -> output` и ограничения ручной реализации.

Kafka Streams как Java API разбирается теоретически. Демонстрационная обработка выполняется Python consumer-producer приложением.

Python stream processor:

```python
# file: python_stream_processor.py
import json
import os
import time
from collections import defaultdict

from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

# Загрузка .env.
load_dotenv()

# Input и output topics.
input_topic = os.environ["TELEMETRY_TOPIC"]
output_topic = "telemetry-1m"

# Consumer получает telemetry events.
consumer = Consumer(
    {
        # Kafka broker.
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        # Отдельная group для stream processor.
        "group.id": "python-telemetry-aggregator",
        # Читать с начала при первом запуске.
        "auto.offset.reset": "earliest",
        # Commit после публикации aggregate.
        "enable.auto.commit": False,
    }
)

# Producer публикует агрегаты.
producer = Producer(
    {
        # Kafka broker.
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        # Подтверждение записи leader.
        "acks": "1",
    }
)

consumer.subscribe([input_topic])
counts = defaultdict(int)
window_started = int(time.time() // 60 * 60)

while True:
    msg = consumer.poll(1.0)
    now_window = int(time.time() // 60 * 60)

    if now_window != window_started:
        for device_id, count in counts.items():
            aggregate = {
                "window_start": window_started,
                "device_id": device_id,
                "events": count,
            }
            producer.produce(
                output_topic,
                key=device_id,
                value=json.dumps(aggregate),
            )
        producer.flush()
        counts.clear()
        window_started = now_window

    if msg is None:
        continue
    if msg.error():
        print(f"error={msg.error()}")
        continue

    event = json.loads(msg.value().decode("utf-8"))
    device_id = event["device_id"]
    counts[device_id] += 1
    consumer.commit(message=msg)
```

Проверка:

```bash
# Input topic.
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --create \
  --topic telemetry \
  --partitions 3 \
  --replication-factor 1

# Output topic.
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --create \
  --topic telemetry-1m \
  --partitions 3 \
  --replication-factor 1
```
## 10. TLS и ACL

### Теория

Безопасность Kafka состоит из transport security, authentication и authorization. TLS защищает сетевой канал, SASL или mTLS определяют principal, ACL разрешают или запрещают операции над topics, groups, cluster и transactional ids. Ошибки TLS возникают до авторизации, ошибки ACL - после успешного подключения.

Security в Kafka состоит из transport security, authentication и authorization. PLAINTEXT не шифрует трафик. SSL включает TLS. SASL_PLAINTEXT выполняет SASL-аутентификацию без TLS и поэтому не защищает пароль в сети. SASL_SSL совмещает SASL и TLS.

Authentication отвечает на вопрос "кто подключился". Authorization отвечает на вопрос "что этому principal разрешено". Principal может появляться из TLS certificate, SASL username, Kerberos principal или OAuth subject.

ACL задаются на resources: topic, group, cluster, transactional id. Producer без `Describe` может не получить metadata topic. Consumer без `Read` на group не сможет фиксировать offsets и участвовать в group. Admin principal должен быть добавлен в `super.users` или иметь достаточные ACL, иначе можно заблокировать управление кластером.

Последовательность security checks:

```text
network connection
  -> TLS handshake
  -> authentication principal
  -> Kafka request parsing
  -> ACL authorization
  -> operation execution
```

TLS error означает проблему сертификатов, truststore, hostname или protocol mismatch. Authorization error означает, что подключение успешно, principal определен, но ACL запрещает операцию.

### Техническая модель

Безопасность Kafka состоит из нескольких независимых слоев. TLS шифрует сетевой трафик и может проверять сертификаты. SASL отвечает за аутентификацию пользователя или сервиса. ACL отвечает за авторизацию операций над Kafka resources: topics, groups, cluster, transactional ids.

### Контрольные положения

- TLS без ACL защищает канал, но не ограничивает операции пользователя.
- ACL без TLS не защищает трафик от чтения в сети.
- `ssl.client.auth=required` включает mTLS и позволяет использовать certificate principal.
- `allow.everyone.if.no.acl.found=false` делает модель строгой: нет ACL - нет доступа.
- Producer для записи в topic обычно требует `Write` и `Describe`.
- Consumer требует `Read`/`Describe` на topic и `Read` на consumer group.
- Ошибки TLS возникают до Kafka protocol authorization, ошибки ACL возникают после успешного подключения.

### Последовательность настройки и проверки

Создаем CA и broker certificate, запускаем SSL listener, проверяем Python SSL producer, затем добавляем ACL и повторяем запись. Демонстрация разделяет транспортную доступность и разрешения на Kafka operations.

Технические состояния:

```text
Broker слушает SSL listener.
Client properties содержит truststore и keystore.
ACL authorizer включен.
Producer principal имеет Write/Describe на topic.
Consumer principal имеет Read/Describe на topic и Read на group.
```

Admin client config:

```properties
# security.protocol
# Варианты: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
# Влияние: должен совпадать с listener broker; SSL включает TLS, SASL_SSL добавляет authentication поверх TLS.
security.protocol=SSL

# ssl.truststore.location
# Варианты: путь к JKS или PKCS12 truststore.
# Влияние: содержит CA broker; без него client не сможет проверить сертификат broker.
ssl.truststore.location=/etc/kafka/ssl/kafka.truststore.p12

# ssl.truststore.password
# Варианты: строка-пароль truststore.
# Влияние: неверный пароль приводит к ошибке чтения truststore до подключения к Kafka.
ssl.truststore.password=changeit

# ssl.keystore.location
# Варианты: путь к client keystore; нужен при ssl.client.auth=required.
# Влияние: содержит client certificate/private key; principal из сертификата может использоваться в ACL.
ssl.keystore.location=/etc/kafka/ssl/admin.keystore.p12

# ssl.keystore.password
# Варианты: строка-пароль keystore.
# Влияние: нужен для чтения client certificate/key.
ssl.keystore.password=changeit

# ssl.key.password
# Варианты: строка-пароль private key.
# Влияние: нужен, если private key в keystore защищен отдельным паролем.
ssl.key.password=changeit

# ssl.endpoint.identification.algorithm
# Варианты: HTTPS или пустая строка.
# Влияние: HTTPS проверяет hostname по сертификату; пустая строка отключает проверку и допустима только в изолированном localhost-стенде.
ssl.endpoint.identification.algorithm=
```

Python client SSL config:

```python
# Общий SSL config для confluent-kafka.
ssl_config = {
    # SSL transport.
    "security.protocol": "SSL",
    # CA certificate в PEM.
    "ssl.ca.location": "/etc/kafka/ssl/ca.crt",
    # Client certificate в PEM.
    "ssl.certificate.location": "/etc/kafka/ssl/producer.crt",
    # Client private key в PEM.
    "ssl.key.location": "/etc/kafka/ssl/producer.key",
    # Пароль ключа, если ключ зашифрован.
    "ssl.key.password": "changeit",
    # Отключение hostname verification для локального стенда.
    "ssl.endpoint.identification.algorithm": "none",
}
```

Проверка ACL:

```bash
# Producer principal получает запись в secure-events.
"$KAFKA_HOME/bin/kafka-acls.sh" \
  --bootstrap-server localhost:9093 \
  --command-config admin-client.properties \
  --add \
  --allow-principal "User:CN=producer" \
  --operation Write \
  --operation Describe \
  --topic secure-events

# Consumer principal получает чтение topic.
"$KAFKA_HOME/bin/kafka-acls.sh" \
  --bootstrap-server localhost:9093 \
  --command-config admin-client.properties \
  --add \
  --allow-principal "User:CN=consumer" \
  --operation Read \
  --operation Describe \
  --topic secure-events

# Consumer principal получает чтение group.
"$KAFKA_HOME/bin/kafka-acls.sh" \
  --bootstrap-server localhost:9093 \
  --command-config admin-client.properties \
  --add \
  --allow-principal "User:CN=consumer" \
  --operation Read \
  --group secure-readers
```

Ожидаемые ошибки при нарушении ACL:

```text
TopicAuthorizationException
GroupAuthorizationException
ClusterAuthorizationException
```
## 11. Monitoring

### Теория

Мониторинг Kafka должен покрывать broker health, partition health, consumer health, storage health и JVM health. Consumer lag относится к конкретной group и отражает отставание от конца log. Exporters дают метрики, а Kafka CLI используется для контрольной диагностики.

Мониторинг Kafka разделяется на broker health, partition health, consumer health, storage health и JVM health. Нельзя ограничиваться только доступностью процесса. Broker может быть запущен, но иметь offline partitions, высокий request latency или переполненный диск.

Consumer lag равен разнице между `LOG-END-OFFSET` и `CURRENT-OFFSET` для group. Lag относится к конкретной group, а не ко всей Kafka. Если одна group отстает, другие groups могут работать нормально. Причина lag часто находится в downstream: БД, HTTP API, CPU обработки, размер batch или ошибки сериализации.

JMX Exporter и Kafka Exporter решают разные задачи. JMX Exporter отдает broker/JVM metrics, Kafka Exporter удобен для lag и metadata. CLI-команды остаются контрольным источником при разборе инцидента.

Минимальная карта диагностики:

```text
broker unavailable       -> process, port, logs, controller metadata
produce latency high     -> request latency, ISR, disk IO, acks
under-replicated topic   -> broker health, network, follower fetch
consumer lag high        -> input rate, processing time, downstream state
disk pressure            -> retention, partitions, replicas, segment size
```

Consumer lag без ошибок broker обычно разбирается со стороны consumer-приложения и downstream-системы.

### Техническая модель

Мониторинг Kafka строится вокруг broker metrics, JVM metrics, состояния partitions и consumer lag. JMX Exporter переводит JMX MBeans в Prometheus format. Kafka Exporter дополнительно собирает consumer group lag и metadata topic/partition. CLI-команды Kafka остаются первичной проверкой при расхождении метрик и фактического состояния.

### Контрольные положения

- `up=1` у exporter означает доступность exporter, а не корректность Kafka.
- Under-replicated partitions означают, что followers отстали или недоступны.
- Offline partitions означают отсутствие leader и недоступность чтения/записи по partition.
- Consumer lag измеряет отставание group, а не broker.
- Lag без роста input может означать зависший consumer; lag при высоком input может означать недостаточный throughput обработки.
- JVM heap и GC влияют на latency broker, но Kafka также критично зависит от page cache.

### Последовательность настройки и проверки

Запускаем JMX Exporter, проверяем endpoint `/metrics`, добавляем scrape config Prometheus и связываем consumer lag из CLI с метриками. Блок фиксирует минимальный набор сигналов для эксплуатации.

JMX Exporter JVM option:

```bash
# JMX Exporter Java agent для Kafka broker.
export KAFKA_OPTS="-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=7071:/opt/jmx-exporter/kafka-broker.yml"
```

Kafka Exporter запуск:

```bash
# Exporter consumer lag и topic metadata.
kafka_exporter \
  --kafka.server=localhost:9092 \
  --web.listen-address=:9308
```

Prometheus config:

```yaml
global:
  # scrape_interval
  # Варианты: duration, например 5s, 15s, 1m.
  # Влияние: частота опроса targets; меньшее значение детальнее фиксирует всплески, но повышает нагрузку на Prometheus/exporters.
  scrape_interval: 15s

scrape_configs:
  # job_name
  # Варианты: произвольное имя job.
  # Влияние: попадает в label job; удобно отделять Kafka JMX metrics от exporter metrics.
  - job_name: kafka-jmx
    static_configs:
      # targets
      # Варианты: список endpoints JMX Exporter.
      # Влияние: up=0 означает недоступность exporter endpoint, а не обязательно падение Kafka broker.
      - targets:
          - localhost:7071

  # job_name
  # Варианты: отдельное имя job для Kafka Exporter.
  # Влияние: используется для запросов consumer lag и topic metadata.
  - job_name: kafka-exporter
    static_configs:
      # targets
      # Варианты: endpoints Kafka Exporter.
      # Влияние: недоступность target скрывает lag metrics; broker может продолжать работать.
      - targets:
          - localhost:9308
```

Проверки:

```bash
# Метрики JMX Exporter.
curl -s http://localhost:7071/metrics | grep kafka_server_brokertopicmetrics

# Метрики Kafka Exporter.
curl -s http://localhost:9308/metrics | grep kafka_consumergroup_lag
```
## 12. Финальные сценарии на Python

### Теория

Практический Kafka pipeline проектируется от события: producer публикует факт, topic хранит поток, несколько consumer groups независимо выполняют side effects и публикуют производные события. Надежность строится через idempotent producer, replication, manual commit и идемпотентные downstream-операции.

Практический Kafka-сценарий проектируется от события. Для каждого события определяются producer, key, payload schema, retention, replication factor, consumers и права доступа. Если событие важно для бизнеса, оно должно иметь `event_id`, временную метку, тип, версию схемы и поля, достаточные для обработки downstream.

Несколько consumer groups не конкурируют за сообщения. Billing, shipping и notifications могут читать `orders` независимо, потому что каждая group имеет свои offsets. Ошибка shipping не должна останавливать billing.

Side effects делают consumers сложнее producers. Если consumer списывает деньги, создает доставку или пишет в БД, повторная обработка должна быть безопасной. Поэтому нужны idempotency keys, уникальные constraints, inbox/outbox tables или проверка уже обработанных `event_id`.

Антипаттерны для разбора: Kafka как синхронный RPC, ожидание глобального порядка во всем topic, auto commit до внешней записи, отсутствие idempotency key, слишком мало partitions для высокого потока, слишком много partitions без capacity planning.

Шаблон проектирования topic:

```text
name:
  orders
event types:
  OrderCreated, OrderCancelled
key:
  order_id
ordering:
  within order_id
partitions:
  based on target consumer parallelism
replication:
  factor 3, min.insync.replicas 2
retention:
  based on replay and recovery window
schema:
  versioned JSON/Avro/Protobuf
security:
  producer Write/Describe, consumer Read/Describe, group Read
```

Consumer side effects фиксируются отдельно: external transaction boundary, idempotency key, retry policy, commit offset point, dead-letter topic policy.

### Техническая модель

Финальные сценарии объединяют Kafka topics, Python producers, Python consumers, consumer groups, offsets, внешние состояния и monitoring. Назначение технической сборки - зафиксировать, как одно событие становится источником нескольких независимых процессов и как Kafka отделяет момент приема события от момента обработки.

### Контрольные положения

- Event-driven pipeline строится вокруг факта, который уже произошел, например `OrderCreated`.
- Producer отвечает за надежную публикацию события, consumer отвечает за идемпотентную обработку.
- Несколько consumer groups читают один topic независимо и не конкурируют между собой.
- Output topics фиксируют результаты отдельных стадий обработки.
- Ошибка одного downstream-сервиса не должна останавливать другие consumer groups.
- Для важных событий используется `acks=all`, idempotent producer и replication.
- Для side effects используется manual commit после успешной операции во внешней системе.

### Последовательность настройки и проверки

Собираем Python/FastAPI producer и несколько Python workers. Проверяем, что один HTTP-запрос создает событие заказа, billing и shipping независимо читают это событие и публикуют свои результаты. Затем проверяем consumer group offsets и output topics.

### Система логирования

Поток:

```text
FastAPI service -> Python logging producer -> Kafka app-logs -> Python consumer -> PostgreSQL
```

HTTP-приложение:

```python
# file: app_logging_api.py
import json
import os
import time
from uuid import uuid4

from confluent_kafka import Producer
from dotenv import load_dotenv
from fastapi import FastAPI

# Загрузка .env.
load_dotenv()

# Kafka producer для логов.
producer = Producer(
    {
        # Kafka broker.
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        # Быстрая запись для логов.
        "acks": "1",
        # Сжатие логового потока.
        "compression.type": "lz4",
    }
)

app = FastAPI()


@app.post("/payments")
def create_payment(amount: float):
    # Trace id связывает HTTP request и log event.
    trace_id = str(uuid4())
    event = {
        "ts": time.time(),
        "service": "payments-api",
        "level": "INFO",
        "message": "payment_created",
        "amount": amount,
        "trace_id": trace_id,
    }
    producer.produce(
        os.environ["APP_LOGS_TOPIC"],
        key=trace_id,
        value=json.dumps(event),
    )
    producer.poll(0)
    return {"trace_id": trace_id, "status": "created"}
```

Запуск:

```bash
uvicorn app_logging_api:app --host 0.0.0.0 --port 8000
curl -X POST "http://localhost:8000/payments?amount=1500"
```

### Обработка заказов

Поток:

```text
orders-api -> orders topic
orders topic -> billing group
orders topic -> shipping group
orders topic -> notifications group
```

Проверка независимости groups:

```bash
"$KAFKA_HOME/bin/kafka-consumer-groups.sh" \
  --bootstrap-server localhost:9092 \
  --list

"$KAFKA_HOME/bin/kafka-consumer-groups.sh" \
  --bootstrap-server localhost:9092 \
  --describe \
  --all-groups
```

### IoT pipeline

Поток:

```text
telemetry_generator.py -> telemetry -> python_stream_processor.py -> telemetry-1m
```

Контрольный признак:

```text
telemetry содержит сырые события устройств
telemetry-1m содержит агрегаты по device_id и минутному окну
```
