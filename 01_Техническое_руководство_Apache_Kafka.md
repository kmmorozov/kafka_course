# Техническое руководство Apache Kafka

Все прикладные producer, consumer, HTTP API, обработчики событий и внешние подключения выполняются на Python. Kafka запускается как инфраструктурный сервис, Python-код подключается к Kafka по сетевому протоколу и выполняет чтение, запись, обработку событий, работу с PostgreSQL и проверки безопасности.

Подробный справочник параметров находится в [04_Справочник_конфигураций.md](04_Справочник_конфигураций.md). Режимы работы и прикладные сценарии описаны в [05_Режимы_работы_и_сценарии.md](05_Режимы_работы_и_сценарии.md).

## 1. Введение в Apache Kafka

### Теория

Apache Kafka - платформа событийных потоков и распределенный commit log. Kafka не просто передает сообщения между приложениями, а хранит поток фактов, который можно читать независимо разными consumer groups. Базовые понятия Kafka: event, topic, partition, offset, broker, producer, consumer, replica и ISR.

Kafka нужна, когда несколько систем должны надежно обмениваться событиями, но не должны напрямую зависеть друг от друга по скорости и доступности. Например, сервис заказов создает заказ, сервис биллинга списывает оплату, сервис доставки создает отправление, сервис уведомлений отправляет письмо. Без Kafka сервис заказов должен синхронно вызывать все эти системы. С Kafka сервис заказов записывает событие в Kafka, а остальные системы читают его в своем темпе.

Архитектурно Kafka стоит между системами как долговременный журнал событий. Producer зависит только от доступности Kafka, а не от всех downstream-сервисов. Consumer зависит от Kafka и своего внешнего состояния, например базы данных. Такая схема снижает связанность сервисов и позволяет добавлять новые consumers без изменения producer.

```text
             +------------------+
             | order-service    |
             | producer         |
             +---------+--------+
                       |
                       v
             +---------+--------+
             | Kafka topic      |
             | orders           |
             +----+--------+----+
                  |        |
        +---------+        +----------+
        v                             v
+-------+--------+           +--------+--------+
| billing group  |           | shipping group |
| consumer       |           | consumer       |
+----------------+           +----------------+
```

Самая простая картина движения данных:

```text
producer application -> Kafka broker -> topic partition -> consumer application
```

`Producer application` - приложение, которое отправляет событие. `Kafka broker` - сервер Kafka, который принимает и хранит событие. `Topic partition` - журнал внутри Kafka, куда событие записывается. `Consumer application` - приложение, которое читает событие и выполняет свою обработку.

Apache Kafka - распределенная платформа для хранения и передачи потоков событий. Kafka принимает события от producer-приложений, записывает их в append-only журналы и отдает consumer-приложениям по мере чтения.

Событие, или record, - одна запись в Kafka. Это может быть созданный заказ, строка лога, изменение пользователя, измерение датчика или любое другое описание факта. Событие в Kafka обычно содержит:

- `key` - ключ маршрутизации и группировки.
- `value` - полезную нагрузку сообщения.
- `headers` - служебные пары ключ-значение.
- `timestamp` - время создания или записи события.
- `topic`, `partition`, `offset` - координаты события внутри Kafka.

Основные сущности вводятся по цепочке обработки события:

- `producer` - приложение или процесс, который пишет события в Kafka. Например, Python API заказов может быть producer для события `OrderCreated`.
- `broker` - сервер Kafka. Он принимает события от producers, записывает их на диск и отдает consumers. Один broker - это один запущенный сервер Kafka.
- `cluster` - несколько brokers, работающих вместе. Cluster нужен, чтобы распределить нагрузку и хранить копии данных на разных серверах.
- `topic` - именованный поток событий. Например, `orders` для событий заказов или `app-logs` для логов приложений.
- `partition` - часть topic и отдельный упорядоченный журнал. Topic может быть разбит на несколько partitions, чтобы Kafka могла параллельно писать и читать данные.
- `offset` - номер позиции события внутри partition. Offset указывает положение сообщения в журнале.
- `consumer` - приложение или процесс, который читает события из Kafka. Например, billing-service может читать `orders` и списывать оплату.
- `consumer group` - группа consumers с одним `group.id`, которые совместно читают topic. Kafka распределяет partitions между consumers внутри группы.
- `replica` - копия partition на broker. Replicas нужны для отказоустойчивости.
- `leader` - главная replica partition. Producer и consumer работают через leader.
- `follower` - replica, которая копирует данные с leader.
- `ISR` - in-sync replicas, то есть replicas, которые успевают синхронизироваться с leader и могут безопасно стать новым leader при отказе.

Пример с терминами:

```text
orders topic
  partition 0
    leader replica на broker 1
    follower replica на broker 2
    follower replica на broker 3
    records: offset 0, offset 1, offset 2
```

Если producer отправляет событие заказа в `orders`, Kafka записывает его в одну из partitions. Consumer group `billing` читает это событие и фиксирует offset после обработки. Consumer group `shipping` может независимо прочитать то же событие и иметь свой offset.

Подробные пояснения сущностей:

- `broker` не является очередью сам по себе. Broker хранит набор partitions разных topics, принимает сетевые запросы клиентов, пишет данные в log segments и отдает данные consumers. В кластере один topic обычно распределен по нескольким brokers.
- `topic` является именем потока событий. Topic не хранится одним файлом: он состоит из partitions, а partitions уже размещаются на brokers.
- `partition` задает масштабирование и порядок. Если topic имеет 6 partitions, его могут параллельно читать до 6 active consumers внутри одной consumer group. Порядок между разными partitions не гарантируется.
- `key` влияет на выбор partition. Если producer отправляет одинаковый key и количество partitions не меняется, такие события обычно попадают в одну partition и сохраняют относительный порядок.
- `offset` является технической позицией в partition. Offset не является глобальным номером сообщения во всем topic.
- `consumer group` позволяет нескольким экземплярам одного приложения делить partitions. Разные groups читают один topic независимо и не мешают друг другу.
- `replication` защищает данные от потери broker. Репликация не ускоряет чтение одной partition внутри Kafka: чтение и запись идут через leader replica, followers синхронизируются с leader.
- `ISR` отражает фактическую синхронность replicas. Если replica отстала или broker недоступен, она выходит из ISR и не учитывается для надежной записи при `acks=all`.

Kafka отличается от классических брокеров очередей тем, что сообщения после чтения не удаляются сразу. Срок хранения задается политиками retention. Разные consumer groups могут независимо читать один и тот же topic с разных offset.

Типовые сценарии:

- централизованный сбор логов;
- обмен событиями между микросервисами;
- потоковая аналитика;
- IoT-телеметрия;
- CDC из баз данных;
- event-driven architecture.

### Концепция event streaming

Event streaming - способ проектирования систем, при котором приложения обмениваются не командами "сделай действие", а событиями "факт уже произошел". Команда `CreateOrder` просит создать заказ. Событие `OrderCreated` фиксирует, что заказ уже создан. Kafka хранит именно поток фактов, поэтому разные системы могут читать один и тот же поток и строить на его основе собственное состояние.

Событие должно быть самодостаточным для downstream-системы. В нем обычно есть:

- `event_id` - уникальный идентификатор события для идемпотентности;
- `event_type` - тип события, например `OrderCreated`;
- `schema_version` - версия структуры payload;
- `occurred_at` - время возникновения события в исходной системе;
- бизнес-поля: `order_id`, `customer_id`, `amount`;
- технические поля трассировки: `trace_id`, `source`, `correlation_id`.

Kafka отделяет producer от consumers. Producer записывает событие в topic и не обязан знать, сколько downstream-систем его прочитает. Consumer может быть остановлен, обновлен или заменен, а Kafka продолжит хранить события до истечения retention.

### Commit log как основная модель Kafka

Kafka partition - append-only commit log. Новые records добавляются в конец журнала. Уже записанные records не изменяются на месте. Consumer читает журнал с определенной позиции и сам фиксирует прогресс чтения.

Эта модель отличается от классической очереди:

- чтение сообщения не удаляет его из Kafka;
- несколько consumer groups могут независимо читать одни и те же records;
- replay выполняется изменением offset или запуском новой group;
- порядок гарантируется только внутри одного журнала, то есть внутри partition;
- хранение ограничивается retention и compaction, а не фактом чтения.

Commit log делает Kafka удобной для аудита, восстановления состояния, пересчета агрегатов и подключения новых consumers к уже существующему потоку.

## 2. Режимы работы Kafka и клиентов

### Теория

Kafka может работать в разных режимах: одиночный KRaft broker для локального запуска, multi-broker KRaft cluster для отказоустойчивости, legacy ZooKeeper mode для существующих старых инсталляций. Клиенты Kafka также имеют режимы работы: producer выбирает уровень подтверждения записи, consumer выбирает способ commit offset, Kafka Connect выбирает standalone или distributed runtime.

KRaft использует quorum controller-узлов. Quorum означает, что для изменения metadata требуется большинство controller voters. В кластере из одного controller отказ этого процесса останавливает metadata plane. В кластере из трех controllers большинство равно двум, поэтому отказ одного controller не останавливает управление metadata.

```text
1 controller voter  -> majority 1 -> отказ 1 из 1 = quorum недоступен
3 controller voters -> majority 2 -> отказ 1 из 3 = quorum доступен
5 controller voters -> majority 3 -> отказ 2 из 5 = quorum доступен
```

Роли `broker` и `controller` решают разные задачи. Broker хранит пользовательские topics и обслуживает produce/fetch requests. Controller хранит metadata и выполняет leader election. В combined mode один процесс выполняет обе роли. В separated mode controller-процессы не хранят пользовательские partitions.

### Standalone KRaft

Standalone KRaft - один процесс Kafka с ролями `broker,controller`. Этот режим дает один broker и один controller quorum voter без распределенной отказоустойчивости.

Технические свойства:

- один процесс обслуживает clients и metadata;
- `replication.factor=1`;
- отказ процесса останавливает весь стенд;
- внутренние topics создаются с replication factor 1;
- режим подходит для локальной разработки и проверки Python-кода.

### Multi-broker KRaft

Multi-broker KRaft - несколько Kafka nodes с общим controller quorum. В кластере из трех nodes каждый процесс может работать в combined mode `broker,controller`. В production роли могут разделяться: отдельные controller nodes управляют metadata, отдельные broker nodes хранят пользовательские topics.

Технические свойства:

- каждый node имеет уникальный `node.id`;
- все nodes используют один `cluster.id`;
- topic partitions имеют несколько replicas;
- controller выбирает leader для каждой partition;
- отказ broker не обязан останавливать topic, если остаются ISR replicas.

### ZooKeeper mode

ZooKeeper mode - старый режим Kafka, где metadata хранится во внешнем ZooKeeper ensemble. Новые инсталляции Kafka обычно строятся на KRaft. ZooKeeper mode встречается в legacy-кластерах.

В ZooKeeper mode появляется отдельная распределенная система, которую нужно устанавливать, мониторить, резервировать и обновлять. Broker хранит пользовательские данные, а ZooKeeper хранит metadata и участвует в controller election. KRaft убирает этот внешний компонент: metadata становится частью Kafka и хранится в metadata log controller quorum.

### Producer delivery modes

Producer может работать с разным уровнем подтверждения:

- `acks=0` - producer не ждет подтверждения. Максимальная скорость, минимальная надежность.
- `acks=1` - producer ждет запись leader. Быстрее, чем `acks=all`, но acknowledged запись может потеряться при отказе leader до replication.
- `acks=all` - producer ждет подтверждения ISR replicas. Используется для важных событий вместе с `min.insync.replicas`.

Идемпотентный producer (`enable.idempotence=true`) защищает от дублей при retry в рамках producer session. Это не отменяет необходимость идемпотентной бизнес-логики во внешних системах.

### Consumer offset modes

Consumer может фиксировать offsets автоматически или вручную:

- auto commit проще, но offset может быть сохранен до завершения обработки;
- manual commit после обработки дает at-least-once семантику;
- batch commit повышает throughput, но увеличивает объем повторной обработки после сбоя.

Для Python consumers, которые пишут в PostgreSQL или вызывают внешние сервисы, используется manual commit после успешной внешней операции.

### Kafka Connect modes

Kafka Connect имеет два режима:

- standalone - один worker и локальные файлы конфигурации, удобно для разработки;
- distributed - несколько workers с общими internal topics в Kafka, используется для отказоустойчивых интеграций.

Внешние подключения реализуются Python-кодом. Kafka Connect остается production runtime для готовых connectors.

## 3. Установка и первый запуск

### Теория

Kafka broker запускается как JVM-процесс и требует заранее подготовленного хранилища. В KRaft mode перед первым стартом каталог данных форматируется командой `kafka-storage.sh format`, которая записывает `cluster.id` и metadata log. Listener отвечает за сетевой порт, а advertised listener определяет адрес, который Kafka отдаст клиентам в metadata.

### Установка Java на Rocky Linux

```bash
# Установка OpenJDK 17.
sudo dnf install -y java-17-openjdk java-17-openjdk-devel

# Проверка версии Java.
java -version
```

Ожидаемый вывод:

```text
openjdk version "17.x.x" ...
OpenJDK Runtime Environment ...
OpenJDK 64-Bit Server VM ...
```

### Установка Kafka

```bash
# Создание системного пользователя.
sudo useradd --system --home-dir /opt/kafka --shell /sbin/nologin kafka

# Загрузка архива Kafka из стабильного архива Apache.
curl -LO "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"

# Распаковка в /opt.
sudo tar -xzf "kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" -C /opt

# Создание стабильной ссылки /opt/kafka.
sudo ln -sfn "/opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" /opt/kafka

# Создание каталогов данных и логов.
sudo mkdir -p /var/lib/kafka/kraft-combined-logs /var/log/kafka

# Назначение владельца.
sudo chown -R kafka:kafka /opt/kafka "/opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" /var/lib/kafka /var/log/kafka
```

### Standalone broker в KRaft

KRaft - режим Kafka без ZooKeeper. Метаданные кластера хранятся в Kafka quorum. В standalone-стенде один процесс выполняет роли `broker` и `controller`.

Конфиг `/opt/kafka/config/kraft/server.properties`:

```properties
# node.id
# Варианты: целое число, уникальное внутри одного KRaft cluster.
# Влияние: broker/controller регистрируется в metadata quorum под этим id; дублирование id ломает регистрацию узлов.
node.id=1

# process.roles
# Варианты: broker, controller, broker,controller.
# Влияние: broker обслуживает клиентов и хранит partitions; controller управляет metadata; broker,controller включает combined mode.
process.roles=broker,controller

# controller.quorum.voters
# Варианты: список node.id@host:port через запятую, например 1@host1:9093,2@host2:9093,3@host3:9093.
# Влияние: задает состав KRaft controller quorum; 1 voter не дает отказоустойчивости, 3 voters выдерживают отказ одного controller.
controller.quorum.voters=1@localhost:9093

# listeners
# Варианты: listenerName://host:port; пустой host в PLAINTEXT://:9092 означает слушать все интерфейсы.
# Влияние: определяет локальные TCP-порты процесса; конфликт портов не даст broker стартовать.
listeners=PLAINTEXT://:9092,CONTROLLER://:9093

# advertised.listeners
# Варианты: listenerName://host:port, доступный клиентам; в Docker/NAT часто отличается от listeners.
# Влияние: этот адрес возвращается clients в metadata; неверный host приводит к ошибкам подключения после bootstrap.
advertised.listeners=PLAINTEXT://localhost:9092

# controller.listener.names
# Варианты: имя listener из listeners, обычно CONTROLLER.
# Влияние: через этот listener controller quorum обменивается metadata; producer/consumer не должны использовать controller listener.
controller.listener.names=CONTROLLER

# listener.security.protocol.map
# Варианты протоколов: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
# Влияние: задает security protocol для каждого listener; при SSL/SASL_SSL потребуются TLS/SASL параметры.
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

# inter.broker.listener.name
# Варианты: имя client/internal listener из listener.security.protocol.map, например PLAINTEXT, SSL, INTERNAL.
# Влияние: используется для replication и broker-to-broker traffic; недоступный listener приводит к выходу replicas из ISR.
inter.broker.listener.name=PLAINTEXT

# log.dirs
# Варианты: один каталог или список каталогов через запятую.
# Влияние: хранит partition logs, indexes и metadata log; потеря каталога при replication.factor=1 означает потерю данных.
log.dirs=/var/lib/kafka/kraft-combined-logs

# num.partitions
# Варианты: целое число от 1 и выше.
# Влияние: partitions по умолчанию для topics без явного --partitions; больше partitions дают параллелизм, но увеличивают metadata и число файлов.
num.partitions=1

# offsets.topic.replication.factor
# Варианты: 1 для single-broker, 3 для production-кластера из трех brokers.
# Влияние: replication factor topic __consumer_offsets; значение больше числа brokers не даст consumer groups нормально стартовать.
offsets.topic.replication.factor=1

# transaction.state.log.replication.factor
# Варианты: 1 для single-broker, 3 для production.
# Влияние: replication factor transaction state log; нужен transactional producers и exactly-once flows.
transaction.state.log.replication.factor=1

# transaction.state.log.min.isr
# Варианты: 1 для single-broker, обычно 2 при replication factor 3.
# Влияние: минимальное число ISR для transaction state; при недостаточном ISR transactional writes будут отклоняться.
transaction.state.log.min.isr=1

# log.retention.check.interval.ms
# Варианты: положительное число миллисекунд.
# Влияние: частота проверки closed segments на удаление по retention; меньшее значение быстрее очищает данные, но повышает фоновую нагрузку.
log.retention.check.interval.ms=300000
```

Первичная инициализация хранилища:

```bash
# Генерация идентификатора кластера.
KAFKA_CLUSTER_ID="$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)"

# Форматирование каталога данных под KRaft metadata log.
sudo -u kafka "$KAFKA_HOME/bin/kafka-storage.sh" format \
  -t "$KAFKA_CLUSTER_ID" \
  -c "$KAFKA_HOME/config/kraft/server.properties"
```

Ожидаемый вывод:

```text
Formatting /var/lib/kafka/kraft-combined-logs with metadata.version ...
```

Запуск broker:

```bash
sudo -u kafka "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_HOME/config/kraft/server.properties"
```

Проверка broker из другого терминала:

```bash
"$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server localhost:9092
```

Ожидаемый вывод содержит broker `localhost:9092` и список API:

```text
localhost:9092 (id: 1 rack: null) -> (
  Produce(0): ...
  Fetch(1): ...
)
```

Создание topic:

```bash
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

## 4. Topics, partitions, replicas, offsets

### Теория

Topic является логическим потоком событий, partition является физическим журналом внутри topic, offset является позицией record внутри partition. Порядок гарантируется только в пределах одной partition. Key управляет выбором partition и используется для сохранения порядка по бизнес-сущности. Replication factor создает копии partitions на разных brokers, а ISR отражает, какие replicas сейчас синхронны.

Проектирование topic начинается с выбора бизнес-сущности и key. Если нужно сохранить порядок событий заказа, key выбирается как `order_id`. Если нужно равномерно распределить нагрузку и порядок по заказу не критичен, key может быть `customer_id` или отсутствовать. Key влияет на partitioning, а partitioning влияет на порядок, параллелизм и равномерность нагрузки.

```text
record key=order-1 -> hash(key) -> partition 0
record key=order-2 -> hash(key) -> partition 2
record key=order-1 -> hash(key) -> partition 0

partition 0 сохраняет порядок событий order-1
partition 2 хранит независимый журнал order-2
```

Увеличение partitions меняет распределение новых key. Старые records остаются в прежних partitions, новые records с тем же key могут начать попадать в другую partition. Это влияет на строгий порядок по key, если порядок нужен на всей истории сущности.

Topic делится на partitions. Каждая partition является упорядоченным журналом. Порядок сообщений гарантирован только внутри одной partition.

Topic является логическим именем потока. Физически topic состоит из набора каталогов partitions на brokers. Например, topic `orders` с тремя partitions хранится как три независимых журнала: `orders-0`, `orders-1`, `orders-2`. Каждый журнал имеет собственные offsets и собственную leader replica.

Topic проектируется под тип события и жизненный цикл данных. Разные retention, разные схемы сообщений, разные требования к доступу и разные consumers обычно означают разные topics. Не стоит складывать несвязанные события в один topic только ради уменьшения количества topics.

Partition является единицей:

- порядка сообщений;
- параллелизма чтения consumer group;
- репликации;
- leader election;
- хранения log segments на диске.

Количество partitions нужно выбирать по требуемому параллелизму и объему потока. Topic с 12 partitions может иметь до 12 активно читающих consumers внутри одной group. Если consumers больше, чем partitions, лишние consumers будут без assignment. Слишком большое число partitions увеличивает число файлов, metadata, нагрузку на controller и длительность recovery.

Producer выбирает partition:

- по явному номеру partition;
- по hash от `key`;
- по стратегии partitioner при отсутствии key.

Key не является обязательным, но он критичен для порядка по сущности. Если key равен `order_id`, события одного заказа обычно попадают в одну partition. Если key равен `customer_id`, сохраняется порядок событий одного клиента. Если key отсутствует, producer распределяет records без гарантии порядка для конкретной бизнес-сущности.

Offset - координата record внутри partition. Offset `15` в partition `0` и offset `15` в partition `1` - разные сообщения. Offset не является бизнес-идентификатором и не должен использоваться как `order_id` или `event_id`.

Replication factor задает количество копий каждой partition. При `replication.factor=3` каждая partition хранится на трех brokers. Один broker содержит leader replica, остальные содержат follower replicas.

ISR содержит replicas, которые успевают синхронизироваться с leader. Если leader недоступен, controller выбирает нового leader из ISR.

Поля `Leader`, `Replicas` и `Isr` в описании topic отражают фактическое состояние:

- `Leader` - broker id, который принимает чтение и запись по partition;
- `Replicas` - broker ids, где размещены copies partition;
- `Isr` - replicas, которые Kafka считает синхронными с leader;
- если `Isr` меньше `Replicas`, часть replicas отстала или недоступна;
- если leader отсутствует, partition становится недоступной для чтения и записи.

Просмотр topics:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --list
```

Описание topic:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic test-events
```

Типовой вывод:

```text
Topic: test-events TopicId: ... PartitionCount: 1 ReplicationFactor: 1 Configs:
  Topic: test-events Partition: 0 Leader: 1 Replicas: 1 Isr: 1
```

Создание topic с тремя partitions:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 1
```

Увеличение количества partitions:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic orders \
  --partitions 6
```

Количество partitions можно увеличить, но нельзя уменьшить штатной командой. Увеличение partitions меняет распределение новых сообщений по ключам.

## 5. Producers и consumers

### Теория

Producer сериализует record, выбирает partition, формирует batch и отправляет его leader broker. Надежность producer определяется `acks`, retries, idempotence и состоянием ISR. Consumer читает partitions, входит в consumer group и фиксирует offsets. Commit offset не удаляет сообщение из Kafka, а только сохраняет позицию чтения конкретной group.

Producer работает асинхронно: вызов отправки обычно кладет record во внутренний буфер, а сетевой поток Kafka client отправляет batch позже. Delivery callback сообщает результат записи: topic, partition, offset или ошибку. Поэтому корректное producer-приложение должно обрабатывать ошибки доставки, flush при завершении и backpressure при заполнении буфера.

Consumer работает циклом `poll -> process -> commit`. `poll()` получает records и поддерживает участие consumer в group. Если обработка длится слишком долго и consumer не вызывает `poll()`, group coordinator считает consumer зависшим и запускает rebalance.

```text
poll records
  -> deserialize
  -> validate
  -> write external state
  -> commit offset
```

Если внешняя запись завершилась успешно, а commit offset не успел выполниться из-за сбоя, record будет прочитан повторно. Поэтому side effect должен быть идемпотентным.

Producer отправляет сообщения в topic. Важные параметры producer:

- `acks` - подтверждение записи.
- `batch.size` - размер batch перед отправкой.
- `linger.ms` - ожидание накопления batch.
- `compression.type` - сжатие сообщений.
- `enable.idempotence` - идемпотентная запись.
- `retries` - повтор отправки при временных ошибках.

Producer внутри выполняет несколько этапов: сериализует key/value в bytes, получает metadata кластера, выбирает partition, накапливает records в batch, сжимает batch при включенной compression, отправляет request leader broker и получает ack или ошибку. Поэтому задержка producer зависит не только от сети, но и от batching, compression, `acks`, доступности leader и состояния ISR.

`acks` задает момент, когда producer считает запись успешной:

- `acks=0` - подтверждение не ожидается, потеря сообщений возможна без ошибки на стороне приложения;
- `acks=1` - запись подтверждается leader, но может потеряться при отказе leader до replication;
- `acks=all` - запись подтверждается ISR replicas, надежность зависит от `min.insync.replicas`.

`enable.idempotence=true` защищает от дублей при retry в рамках producer session. Это не делает бизнес-операцию exactly-once во внешней системе. Если consumer пишет в PostgreSQL или вызывает API, ему все равно нужен `event_id`, unique constraint, UPSERT или другой механизм идемпотентности.

Consumer читает сообщения из partitions. Важные параметры consumer:

- `group.id` - идентификатор consumer group.
- `enable.auto.commit` - автоматический commit offset.
- `auto.offset.reset` - стартовая позиция при отсутствии committed offset.
- `max.poll.records` - максимум сообщений за один poll.
- `isolation.level` - чтение транзакционных сообщений.

Consumer group хранит committed offsets во внутреннем topic `__consumer_offsets`. Commit offset не удаляет сообщение из Kafka. Он только фиксирует, что конкретная group считает сообщения до этой позиции обработанными. Другая group может читать тот же topic с начала или с другой позиции.

Rebalancing происходит, когда consumer входит в group, выходит из group, перестает отправлять heartbeats или меняется число partitions. Во время rebalance Kafka перераспределяет partitions между members group. Частые rebalances ухудшают задержку и throughput, поэтому обработка batch не должна превышать `max.poll.interval.ms`.

Serialization важна, потому что Kafka хранит bytes. JSON удобен для чтения человеком и отладки, но не дает строгой схемы сам по себе. Avro и Protobuf обычно применяются там, где нужна формальная schema evolution. При изменении структуры события нельзя ломать старые consumers: безопаснее добавлять optional fields, чем менять тип или удалять существующее поле.

### Console producer

```bash
"$KAFKA_HOME/bin/kafka-console-producer.sh" \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --property parse.key=true \
  --property key.separator=:
```

Ввод:

```text
order-1:{"order_id":1,"amount":1500}
order-2:{"order_id":2,"amount":2700}
order-1:{"order_id":1,"amount":1900}
```

`parse.key=true` отделяет key от value по символу `:`. Сообщения с одинаковым key попадают в одну partition при неизменном количестве partitions.

### Console consumer

```bash
"$KAFKA_HOME/bin/kafka-console-consumer.sh" \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group orders-reader \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true
```

Типовой вывод:

```text
Partition:0 Offset:0 order-1 {"order_id":1,"amount":1500}
Partition:2 Offset:0 order-2 {"order_id":2,"amount":2700}
Partition:0 Offset:1 order-1 {"order_id":1,"amount":1900}
```

### Delivery semantics

`at-most-once`: offset фиксируется до обработки. При сбое сообщение может быть потеряно для приложения.

`at-least-once`: offset фиксируется после обработки. При сбое сообщение может быть обработано повторно.

`exactly-once`: producer idempotence, transactions и чтение `read_committed` обеспечивают согласованную запись результатов в Kafka.

Exactly-once в Kafka относится к операциям внутри Kafka: чтение input topic, запись output topic и commit offsets могут быть объединены Kafka transactions. Если обработка включает PostgreSQL, HTTP API или файловую систему, Kafka transaction не покрывает эту внешнюю операцию. Для внешних систем используются transactional outbox, inbox table, idempotency keys, unique constraints и повторяемые операции.

### Producer properties

```properties
# bootstrap.servers
# Варианты: один host:port или несколько через запятую.
# Влияние: используется для начального подключения и получения metadata; в production указывают несколько brokers для устойчивого старта клиента.
bootstrap.servers=localhost:9092

# acks
# Варианты: 0, 1, all или -1.
# Влияние: 0 не ждет broker и может терять сообщения; 1 ждет leader; all ждет ISR и дает надежную запись вместе с min.insync.replicas.
acks=all

# enable.idempotence
# Варианты: true, false.
# Влияние: true защищает от дублей при retry в рамках producer session; требует совместимых acks/retries и повышает надежность записи.
enable.idempotence=true

# retries
# Варианты: 0 и выше.
# Влияние: больше retries повышает шанс доставки при временных ошибках; без идемпотентности retries могут приводить к дублям.
retries=10

# compression.type
# Варианты: none, gzip, snappy, lz4, zstd.
# Влияние: снижает сетевой трафик и размер записи на диск; gzip сильнее грузит CPU, lz4 часто дает хороший баланс скорости и сжатия.
compression.type=lz4

# batch.size
# Варианты: положительное число байт.
# Влияние: больший batch повышает throughput и эффективность compression, но может увеличить latency и memory usage producer.
batch.size=32768

# linger.ms
# Варианты: 0 и выше, миллисекунды.
# Влияние: 0 минимизирует задержку; 5-20 ms часто повышают throughput за счет накопления batch.
linger.ms=10

# key.serializer
# Варианты: Java serializer class, например StringSerializer, ByteArraySerializer.
# Влияние: преобразует key в bytes; key влияет на partitioning и compaction.
key.serializer=org.apache.kafka.common.serialization.StringSerializer

# value.serializer
# Варианты: Java serializer class, например StringSerializer, ByteArraySerializer, Avro serializer.
# Влияние: определяет формат payload в Kafka; должен быть совместим с deserializer consumer.
value.serializer=org.apache.kafka.common.serialization.StringSerializer
```

### Consumer properties

```properties
# bootstrap.servers
# Варианты: один host:port или несколько через запятую.
# Влияние: используется для начального подключения; после получения metadata consumer подключается к leaders нужных partitions.
bootstrap.servers=localhost:9092

# group.id
# Варианты: непустая строка.
# Влияние: consumers с одним group.id делят partitions; разные group.id читают topic независимо и имеют отдельные offsets.
group.id=orders-service

# enable.auto.commit
# Варианты: true, false.
# Влияние: true фиксирует offsets по таймеру; false требует явного commit после обработки и нужен для at-least-once с внешними системами.
enable.auto.commit=false

# auto.offset.reset
# Варианты: earliest, latest, none в Java client; в librdkafka также используется error.
# Влияние: earliest читает историю с начала при новом group.id; latest читает только новые сообщения; none/error дает ошибку без offset.
auto.offset.reset=earliest

# max.poll.records
# Варианты: положительное целое число.
# Влияние: больше records за poll повышает throughput, но увеличивает время обработки batch и риск превышения max.poll.interval.ms.
max.poll.records=100

# key.deserializer
# Варианты: Java deserializer class, например StringDeserializer, ByteArrayDeserializer.
# Влияние: преобразует key bytes в объект; должен соответствовать producer key.serializer.
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer

# value.deserializer
# Варианты: Java deserializer class, например StringDeserializer, ByteArrayDeserializer, Avro deserializer.
# Влияние: преобразует payload bytes; несовместимый deserializer приводит к ошибкам обработки.
value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

## 6. Kafka cluster

### Теория

Kafka cluster распределяет partitions между brokers. Для каждой partition выбирается leader replica, через которую идут чтение и запись, а follower replicas копируют данные. Controller управляет metadata, leader election и состоянием ISR. В KRaft mode metadata хранится внутри Kafka controller quorum, без внешнего ZooKeeper.

Клиент Kafka не отправляет все данные на первый broker из `bootstrap.servers`. Bootstrap broker нужен для получения metadata. После этого producer и consumer подключаются к brokers, которые являются leaders нужных partitions.

```text
client -> bootstrap broker -> metadata
metadata:
  orders-0 leader broker 1
  orders-1 leader broker 2
  orders-2 leader broker 3

producer -> broker 1 for orders-0
producer -> broker 2 for orders-1
producer -> broker 3 for orders-2
```

Отказ broker влияет только на partitions, где этот broker был leader или replica. Controller выбирает новых leaders из ISR. Если ISR для partition пустой или меньше требований `min.insync.replicas`, запись останавливается до восстановления синхронных replicas.

Кластер Kafka состоит из нескольких brokers. Каждый broker хранит часть partitions. Controller управляет метаданными: создает partitions, отслеживает brokers, выполняет leader election.

В KRaft controller quorum состоит из одного или нескольких controller-процессов. В кластере из трех nodes каждый процесс может быть одновременно broker и controller.

Controller хранит и изменяет metadata кластера: brokers, topics, partitions, replicas, leaders, ISR, конфигурации. Producer не отправляет каждое сообщение через controller. Producer получает metadata, затем пишет напрямую leader broker нужной partition. Controller становится критичным при изменении metadata и отказах, когда нужно выбрать нового leader.

В KRaft существуют три схемы ролей:

- `broker,controller` - combined mode для небольшого кластера или локального стенда;
- `broker` - узел хранит пользовательские topics и обслуживает clients;
- `controller` - узел участвует только в metadata quorum.

Separated mode сложнее в настройке, но изолирует metadata plane от клиентской нагрузки. В production это снижает риск, что тяжелый produce/fetch traffic повлияет на controller quorum.

Leader election выполняется при отказе leader broker или административных изменениях. Нормальный leader election выбирает новую leader replica из ISR. Unclean leader election может выбрать replica вне ISR и тем самым повысить доступность ценой возможной потери acknowledged records. Для критичных данных unclean election отключают.

Фрагмент конфига broker 1:

```properties
# node.id
# Варианты: уникальное целое число для каждого узла, например 1, 2, 3.
# Влияние: должно совпадать с id в controller.quorum.voters; дублирование id нарушает работу quorum и broker registration.
node.id=1

# process.roles
# Варианты: broker, controller, broker,controller.
# Влияние: combined mode broker,controller снижает число процессов; separated mode снижает влияние client traffic на controllers.
process.roles=broker,controller

# controller.quorum.voters
# Варианты: полный список controller nodes в формате nodeId@host:port.
# Влияние: список должен быть одинаковым на всех nodes; кластер из трех voters выдерживает отказ одного controller.
controller.quorum.voters=1@localhost:19093,2@localhost:29093,3@localhost:39093

# listeners
# Варианты: несколько listenerName://host:port, например PLAINTEXT, SSL, CONTROLLER.
# Влияние: задает локальные порты; у brokers на одном сервере порты должны отличаться.
listeners=PLAINTEXT://:19092,CONTROLLER://:19093

# advertised.listeners
# Варианты: адреса, доступные clients и другим brokers.
# Влияние: каждый broker должен публиковать свой адрес; одинаковый advertised address у разных brokers ломает client metadata.
advertised.listeners=PLAINTEXT://localhost:19092

# controller.listener.names
# Варианты: имя controller listener, обычно CONTROLLER.
# Влияние: используется только metadata quorum, не для producer/consumer traffic.
controller.listener.names=CONTROLLER

# listener.security.protocol.map
# Варианты: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL для каждого listener.
# Влияние: определяет безопасность каждого listener; переход на SSL/SASL требует дополнительных TLS/SASL настроек.
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

# inter.broker.listener.name
# Варианты: listener, доступный другим brokers, например PLAINTEXT, INTERNAL, SSL.
# Влияние: через него идет replication; ошибка приводит к уменьшению ISR и сбоям replication.
inter.broker.listener.name=PLAINTEXT

# log.dirs
# Варианты: отдельный каталог или список каталогов на broker.
# Влияние: каждый broker должен иметь свой каталог; общий log.dirs для нескольких brokers приводит к конфликтам lock/data.
log.dirs=/var/lib/kafka/broker-1

# offsets.topic.replication.factor
# Варианты: 1 для single-broker, 3 для трех brokers.
# Влияние: защищает committed offsets consumer groups; значение больше числа brokers не позволит создать __consumer_offsets.
offsets.topic.replication.factor=3

# transaction.state.log.replication.factor
# Варианты: 1 для single-broker, 3 для production-кластера.
# Влияние: защищает transaction state для transactional producers.
transaction.state.log.replication.factor=3

# transaction.state.log.min.isr
# Варианты: 1..transaction.state.log.replication.factor, обычно 2 при RF=3.
# Влияние: если ISR меньше значения, transactional writes будут отклоняться.
transaction.state.log.min.isr=2
```

Создание replicated topic:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:19092,localhost:29092,localhost:39092 \
  --create \
  --topic payments \
  --partitions 3 \
  --replication-factor 3
```

Описание:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:19092 \
  --describe \
  --topic payments
```

Ожидаемый вид:

```text
Topic: payments PartitionCount: 3 ReplicationFactor: 3
  Partition: 0 Leader: 1 Replicas: 1,2,3 Isr: 1,2,3
  Partition: 1 Leader: 2 Replicas: 2,3,1 Isr: 2,3,1
  Partition: 2 Leader: 3 Replicas: 3,1,2 Isr: 3,1,2
```

При остановке broker, который является leader для части partitions, controller выбирает нового leader из ISR. Topic остается доступным, если для partition остается хотя бы одна синхронная replica и настройки `min.insync.replicas` совместимы с числом доступных brokers.

## 7. Хранение данных и производительность

### Теория

Kafka хранит partitions как набор segment files на диске. Retention удаляет старые closed segments по времени или размеру, compaction оставляет последнее значение для каждого key. Производительность Kafka зависит от последовательного IO, page cache, batch size, compression, количества partitions, replication factor и выбранных гарантий записи.

Файлы partition имеют имена по базовому offset segment. Например, `00000000000000000000.log` содержит records начиная с offset 0, а `00000000000000012345.log` содержит records начиная с offset 12345. Index files ускоряют поиск нужного offset без полного чтения `.log`.

```text
orders-0/
  00000000000000000000.log
  00000000000000000000.index
  00000000000000000000.timeindex
  00000000000000012345.log
  00000000000000012345.index
  00000000000000012345.timeindex
```

Kafka не должна хранить большие binary payloads как основной сценарий. Для крупных файлов обычно используется object storage, а Kafka record содержит ссылку, checksum, metadata и event id. Это снижает нагрузку на brokers, replication и consumers.

Kafka хранит данные в log segments. Partition соответствует каталогу вида `topic-partition`, например `orders-0`. Внутри лежат segment files:

- `.log` - данные сообщений;
- `.index` - индекс offset;
- `.timeindex` - индекс времени;
- `.txnindex` - индекс транзакций.

Retention удаляет старые segments по времени или размеру. Compaction оставляет последнее значение для каждого key и используется для topics с состоянием.

Kafka активно использует page cache операционной системы. Broker пишет и читает последовательные файлы, а ОС держит горячие страницы в памяти. Поэтому Kafka не нужно выделять весь RAM под JVM heap. Слишком большой heap уменьшает page cache и может ухудшить производительность дискового чтения.

Segment - часть partition log. Active segment принимает новые records. Когда достигается `segment.bytes` или `segment.ms`, Kafka закрывает segment и открывает следующий. Retention и compaction работают в основном с closed segments. Поэтому старые records в active segment могут оставаться на диске до закрытия segment.

Retention и compaction решают разные задачи:

- retention ограничивает историю по времени или размеру;
- compaction хранит последнее значение для каждого key;
- `cleanup.policy=delete` подходит для журналов событий;
- `cleanup.policy=compact` подходит для состояния и справочников;
- `cleanup.policy=delete,compact` сочетает хранение последнего состояния и ограничение срока хранения.

Tombstone record в compacted topic - сообщение с key и `value=null`. Оно означает удаление key из состояния. Tombstone хранится минимум `delete.retention.ms`, чтобы consumers при восстановлении успели увидеть факт удаления.

Topic-level настройки:

```properties
# retention.ms
# Варианты: -1 для бессрочного хранения или положительное число миллисекунд.
# Влияние: удаляет closed segments старше указанного срока при cleanup.policy=delete; слишком малое значение может удалить данные раньше чтения consumer.
retention.ms=3600000

# retention.bytes
# Варианты: -1 без ограничения или положительное число байт на partition.
# Влияние: ограничивает размер каждой partition; общий размер topic равен retention.bytes * partitions * replication.factor.
retention.bytes=1073741824

# cleanup.policy
# Варианты: delete, compact, delete,compact.
# Влияние: delete удаляет старые segments; compact оставляет последнее значение key; комбинированный режим делает оба действия.
cleanup.policy=delete

# segment.bytes
# Варианты: положительное число байт.
# Влияние: размер segment file; малые segments быстрее подпадают под retention/compaction, но увеличивают число файлов.
segment.bytes=10485760

# file.delete.delay.ms
# Варианты: положительное число миллисекунд.
# Влияние: задержка перед физическим удалением файлов segment; увеличивает окно, когда удаленные files еще могут быть открыты ОС.
file.delete.delay.ms=60000
```

Создание topic с retention:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --create \
  --topic short-retention \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=60000 \
  --config segment.bytes=1048576
```

Создание compacted topic:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --create \
  --topic user-state \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=compact \
  --config segment.ms=10000 \
  --config min.cleanable.dirty.ratio=0.01
```

Производительность зависит от:

- количества partitions;
- размера batch;
- типа сжатия;
- числа producer и consumer потоков;
- дисковой подсистемы;
- сетевой задержки;
- `acks` и `min.insync.replicas`;
- нагрузки на page cache.

Throughput и latency всегда находятся в компромиссе. Больший `batch.size` и `linger.ms` повышают throughput и качество сжатия, но добавляют задержку. `acks=all` повышает надежность, но требует ожидания replication. Compression снижает network и disk usage, но потребляет CPU на producer и broker/consumer.

## 8. Kafka Connect и внешние подключения через Python

### Теория

Kafka Connect является runtime для интеграции Kafka с внешними системами через connectors, workers и tasks. Source connectors пишут данные из внешней системы в Kafka, sink connectors читают Kafka и пишут во внешнюю систему. Прикладная механика интеграции через Python: чтение PostgreSQL, публикация в Kafka, чтение из Kafka, запись в PostgreSQL и commit offset после успешной внешней операции.

Source-интеграция должна хранить позицию чтения внешней системы. Для polling по таблице это может быть последний `id`. Для CDC это позиция в журнале базы данных. Sink-интеграция должна хранить состояние применения события во внешней системе: primary key, idempotency key, таблица processed events или уникальный constraint.

```text
Source side:
PostgreSQL rows -> read position -> Kafka producer -> topic

Sink side:
topic -> Kafka consumer -> idempotent write -> commit offset
```

Если offset Kafka фиксируется до записи во внешнюю систему, событие может быть потеряно для sink. Если offset фиксируется после записи, событие может повториться, поэтому запись во внешнюю систему должна выдерживать повтор.

Kafka Connect запускает connectors, которые читают данные из внешних систем в Kafka или пишут данные из Kafka во внешние системы. Kafka Connect является штатным компонентом Kafka-платформы, а внешние приложения могут выполнять ту же интеграционную механику через Python-код.

Kafka Connect состоит из workers, connectors и tasks. Worker - процесс runtime. Connector - конфигурация интеграции. Task - исполняемая часть connector, которую Connect может распределить по workers. Converter определяет, как данные превращаются в Kafka records: JSON, Avro, Protobuf, String или bytes.

Source connector должен хранить позицию чтения внешней системы. Для JDBC это может быть incrementing column или timestamp column. Sink connector должен учитывать повторную доставку из Kafka и делать запись во внешнюю систему идемпотентной, если connector или downstream допускает повторы.

Python-интеграции выполняют ту же механику явно: source-приложение читает PostgreSQL, producer публикует событие, sink consumer пишет в PostgreSQL и только потом фиксирует offset. Такой код делает видимыми гарантии доставки, retry и идемпотентность.

Типы connectors:

- source connector - внешняя система -> Kafka;
- sink connector - Kafka -> внешняя система.

Режимы:

- standalone - один worker, конфиг connector передается локально;
- distributed - группа workers хранит конфиги, offsets и status во внутренних topics Kafka.

Distributed worker properties:

```properties
# bootstrap.servers
# Варианты: один broker или список brokers через запятую.
# Влияние: Connect worker использует Kafka для internal topics и connector data; недоступность brokers останавливает tasks.
bootstrap.servers=localhost:9092

# group.id
# Варианты: строка, одинаковая для workers одного Connect cluster.
# Влияние: workers с одинаковым group.id распределяют connector tasks; смена group.id создает новый Connect cluster.
group.id=connect-cluster

# config.storage.topic
# Варианты: имя Kafka topic, обычно compacted.
# Влияние: хранит конфигурации connectors; потеря topic означает потерю конфигураций Connect cluster.
config.storage.topic=connect-configs

# offset.storage.topic
# Варианты: имя Kafka topic.
# Влияние: хранит offsets source connectors; потеря offset может вызвать повторное чтение source.
offset.storage.topic=connect-offsets

# status.storage.topic
# Варианты: имя Kafka topic.
# Влияние: хранит состояние connectors/tasks; используется REST API для отображения RUNNING/FAILED/PAUSED.
status.storage.topic=connect-status

# config.storage.replication.factor
# Варианты: 1 для single-broker, 3 для production.
# Влияние: RF topic с конфигами; значение больше числа brokers не даст topic создаться.
config.storage.replication.factor=1

# offset.storage.replication.factor
# Варианты: 1 для single-broker, 3 для production.
# Влияние: защищает offsets source connectors; низкий RF повышает риск повторной загрузки после отказа.
offset.storage.replication.factor=1

# status.storage.replication.factor
# Варианты: 1 для single-broker, 3 для production.
# Влияние: защищает статусы tasks; влияет на восстановление состояния Connect.
status.storage.replication.factor=1

# key.converter
# Варианты: JsonConverter, StringConverter, ByteArrayConverter, AvroConverter, ProtobufConverter.
# Влияние: определяет формат key в Kafka records; должен быть совместим с consumers.
key.converter=org.apache.kafka.connect.json.JsonConverter

# key.converter.schemas.enable
# Варианты: true, false.
# Влияние: false пишет plain JSON без schema envelope и удобен для Python; true добавляет schema metadata.
key.converter.schemas.enable=false

# value.converter
# Варианты: JsonConverter, StringConverter, ByteArrayConverter, AvroConverter, ProtobufConverter.
# Влияние: определяет формат value; Avro/Protobuf обычно требуют Schema Registry.
value.converter=org.apache.kafka.connect.json.JsonConverter

# value.converter.schemas.enable
# Варианты: true, false.
# Влияние: false упрощает чтение Python-кодом; true нужен schema-aware connectors/pipelines.
value.converter.schemas.enable=false

# plugin.path
# Варианты: один или несколько каталогов через запятую.
# Влияние: Connect ищет connector plugins здесь; после добавления jar обычно нужен restart worker.
plugin.path=/opt/kafka/plugins

# rest.host.name
# Варианты: hostname или IP для REST API.
# Влияние: определяет, где доступно управление connectors.
rest.host.name=localhost

# rest.port
# Варианты: свободный TCP port, обычно 8083.
# Влияние: конфликт порта не даст worker стартовать.
rest.port=8083
```

JDBC source connector читает строки из таблицы и публикует их в topic. JDBC sink connector читает topic и записывает строки в таблицу.

Пример source connector JSON:

```json
{
  "name": "orders-source",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:postgresql://localhost:5432/kafka_lab",
    "connection.user": "kafka",
    "connection.password": "kafka",
    "mode": "incrementing",
    "incrementing.column.name": "id",
    "table.whitelist": "orders",
    "topic.prefix": "pg_",
    "poll.interval.ms": "5000",
    "tasks.max": "1"
  }
}
```

JSON не поддерживает комментарии. Пояснение параметров:

- `connector.class` - класс JDBC source connector.
- `connection.url` - JDBC URL PostgreSQL.
- `mode=incrementing` - новые строки выбираются по возрастающему числовому столбцу.
- `incrementing.column.name=id` - столбец для отслеживания offset.
- `table.whitelist=orders` - таблица-источник.
- `topic.prefix=pg_` - topic будет называться `pg_orders`.
- `poll.interval.ms` - период опроса таблицы.
- `tasks.max` - число параллельных tasks.

### Python producer из PostgreSQL

Зависимости:

```bash
# Создание виртуального окружения для Python-приложений.
python3 -m venv .venv

# Активация окружения.
source .venv/bin/activate

# Kafka client, PostgreSQL client и загрузка .env.
pip install confluent-kafka psycopg[binary] python-dotenv
```

Файл `.env`:

```dotenv
# KAFKA_BOOTSTRAP_SERVERS
# Варианты: один host:port или список brokers через запятую.
# Влияние: Python producer/consumer использует адрес для bootstrap metadata; неверный адрес приводит к connection refused/timeout.
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# KAFKA_TOPIC
# Варианты: имя существующего Kafka topic.
# Влияние: source пишет события в этот topic, sink читает из него; при auto.create.topics.enable=false topic должен быть создан заранее.
KAFKA_TOPIC=pg_orders

# POSTGRES_DSN
# Варианты: postgresql://user:password@host:port/database.
# Влияние: используется psycopg; ошибки user/password/host/database ломают внешнюю интеграцию до Kafka processing.
POSTGRES_DSN=postgresql://kafka:kafka@localhost:5432/kafka_lab
```

Python producer:

```python
# file: pg_orders_producer.py
import json
import os
import time

import psycopg
from confluent_kafka import Producer
from dotenv import load_dotenv

# Загрузка переменных из .env.
load_dotenv()

# Конфигурация producer.
producer = Producer(
    {
        # Адрес Kafka broker.
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        # Подтверждение записи всеми ISR replicas.
        "acks": "all",
        # Идемпотентная отправка защищает от дублей при retry.
        "enable.idempotence": True,
        # Сжатие снижает сетевой трафик.
        "compression.type": "lz4",
    }
)


def delivery_report(error, message):
    # Callback вызывается после подтверждения или ошибки отправки.
    if error is not None:
        print(f"delivery failed: {error}")
        return
    print(
        f"delivered topic={message.topic()} "
        f"partition={message.partition()} offset={message.offset()}"
    )


topic = os.environ["KAFKA_TOPIC"]
dsn = os.environ["POSTGRES_DSN"]
last_id = 0

while True:
    # Подключение создается на итерацию; постоянное подключение требует отдельной обработки reconnect.
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            # Выбор новых строк по монотонно возрастающему id.
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
        # Key задает маршрутизацию по customer_id.
        producer.produce(
            topic=topic,
            key=str(customer_id),
            value=json.dumps(event, ensure_ascii=False),
            callback=delivery_report,
        )
        last_id = order_id

    # poll обслуживает callbacks producer.
    producer.poll(0)
    time.sleep(2)
```

Python consumer в PostgreSQL:

```python
# file: pg_orders_sink.py
import json
import os

import psycopg
from confluent_kafka import Consumer
from dotenv import load_dotenv

# Загрузка .env.
load_dotenv()

# Конфигурация consumer.
consumer = Consumer(
    {
        # Адрес Kafka broker.
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        # Consumer group для sink-приложения.
        "group.id": "orders-python-sink",
        # Новая группа начинает чтение с начала topic.
        "auto.offset.reset": "earliest",
        # Offset фиксируется вручную после записи в PostgreSQL.
        "enable.auto.commit": False,
    }
)

consumer.subscribe([os.environ["KAFKA_TOPIC"]])

with psycopg.connect(os.environ["POSTGRES_DSN"]) as conn:
    while True:
        # poll возвращает одно сообщение или None по timeout.
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"consumer error: {msg.error()}")
            continue

        event = json.loads(msg.value().decode("utf-8"))
        with conn.cursor() as cur:
            # UPSERT делает повторную обработку безопасной.
            cur.execute(
                """
                INSERT INTO processed_orders(id, customer_id, amount, status, created_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET status = EXCLUDED.status,
                    amount = EXCLUDED.amount
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
        # Commit offset выполняется после успешного commit в PostgreSQL.
        consumer.commit(message=msg)
        print(
            f"stored id={event['id']} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )
```

## 9. Kafka Streams

### Теория

Stream processing - непрерывная обработка событий из input topics с записью результата в output topics. Stateless operations не требуют состояния, stateful operations требуют state store, windowing группирует события по времени. Kafka Streams реализует эти принципы как Java-библиотека; Python consumer-producer приложение реализует базовую topology.

Stateful stream processing требует восстановления состояния после сбоя. Kafka Streams решает это через локальный state store и changelog topic. Локальный store дает быстрый доступ к состоянию, changelog topic позволяет восстановить store на другом instance.

```text
input topic -> stream task -> state store -> output topic
                    |
                    v
              changelog topic
```

Windowing зависит от времени события. Если используется timestamp producer, окно строится по времени возникновения события. Если используется broker append time, окно строится по времени записи в Kafka. Для поздних событий нужен grace period, иначе событие может прийти после закрытия окна.

Kafka Streams - Java-библиотека для потоковой обработки данных из Kafka. Приложение Kafka Streams само является Kafka client: читает input topics, строит topology обработки и пишет output topics.

Stream processing отличается от batch processing тем, что приложение постоянно читает поток и обновляет результат по мере прихода событий. Topology описывает граф обработки: source topics, transformations, state stores и sink topics.

Операции делятся на:

- stateless - не требуют памяти о предыдущих событиях, например `filter` и `map`;
- stateful - требуют состояния, например `count`, `aggregate`, `join`;
- windowed - группируют события по временным окнам.

Stateful processing требует state store. Kafka Streams хранит локальное состояние на диске приложения и пишет changelog topic в Kafka, чтобы восстановить состояние после сбоя. Repartition topics появляются, когда для группировки нужно изменить key.

Операции:

- stateless: `filter`, `map`, `flatMap`, `selectKey`;
- stateful: `groupByKey`, `aggregate`, `count`, `join`;
- windowing: tumbling, hopping, sliding, session windows.

Минимальная topology:

```java
StreamsBuilder builder = new StreamsBuilder();

KStream<String, String> source = builder.stream("raw-events");

source
    .filter((key, value) -> value != null && value.contains("ERROR"))
    .to("error-events");

KafkaStreams streams = new KafkaStreams(builder.build(), properties);
streams.start();
```

Kafka Streams создает служебные topics для repartition и state stores. Имя служебных topics содержит `application.id`.

Python consumer-producer приложение реализует базовую topology `input topic -> processor -> output topic`. Для production stateful processing с окнами, joins и exactly-once внутри Kafka применяются Kafka Streams, Flink или другой специализированный stream engine.

## 10. Безопасность Kafka

### Теория

Безопасность Kafka делится на transport security, authentication и authorization. TLS защищает канал, SASL или mTLS определяют, кто подключился, ACL определяют, какие операции разрешены principal. Для producer нужны права на запись в topic, для consumer нужны права на чтение topic и consumer group.

Порядок проверки при подключении:

```text
TCP connection
  -> TLS handshake
  -> authentication principal
  -> Kafka request
  -> ACL authorization
  -> operation result
```

Ошибка TLS возникает до Kafka request: неверный CA, истекший сертификат, несовпадение hostname, недоступный keystore. Ошибка ACL возникает после успешного подключения: principal известен, но операция запрещена.

Principal должен быть стабильным. При mTLS он часто формируется из subject сертификата, например `User:CN=producer`. При SASL/SCRAM principal обычно соответствует username. ACL всегда назначается на конкретный principal и resource.

Уровни безопасности:

- TLS - шифрование трафика и проверка сертификатов.
- SASL - аутентификация через механизм SCRAM, PLAIN, GSSAPI, OAUTHBEARER.
- ACL - авторизация операций над topics, groups, transactional ids и cluster.

Безопасность Kafka разделяется на несколько независимых уровней:

- transport security защищает сетевой канал от чтения и подмены;
- authentication определяет, кто подключился;
- authorization определяет, какие операции разрешены этому principal;
- audit и monitoring фиксируют попытки доступа и ошибки авторизации.

TLS без ACL шифрует канал, но не ограничивает права пользователя. ACL без TLS ограничивает операции, но не защищает трафик от чтения в сети. В production обычно используют `SASL_SSL` или mTLS плюс ACL.

Для producer обычно нужны ACL `Write` и `Describe` на topic. Для consumer нужны `Read` и `Describe` на topic, а также `Read` на consumer group. Для admin-команд нужны cluster-level или resource-level разрешения в зависимости от операции.

TLS listener:

```properties
# listeners
# Варианты: SSL://:9093, SASL_SSL://:9093, несколько listeners через запятую.
# Влияние: включает TLS listener; если порт занят или protocol не описан в listener.security.protocol.map, broker не стартует.
listeners=SSL://:9093

# advertised.listeners
# Варианты: SSL://host:port, доступный clients.
# Влияние: host должен совпадать с сертификатом при включенной hostname verification; неверный адрес ломает подключение clients после metadata.
advertised.listeners=SSL://broker1.example.local:9093

# listener.security.protocol.map
# Варианты: SSL:SSL, INTERNAL:SASL_SSL, CONTROLLER:SSL и другие пары listener:protocol.
# Влияние: определяет security protocol listener; SSL требует keystore/truststore.
listener.security.protocol.map=SSL:SSL

# inter.broker.listener.name
# Варианты: имя listener, доступный другим brokers, например SSL или INTERNAL.
# Влияние: при SSL межброкерная репликация шифруется; ошибки сертификатов приводят к проблемам replication/ISR.
inter.broker.listener.name=SSL

# ssl.keystore.location
# Варианты: путь к JKS или PKCS12 keystore.
# Влияние: содержит private key и certificate broker; отсутствие файла или неверный пароль не даст поднять SSL listener.
ssl.keystore.location=/etc/kafka/ssl/broker1.keystore.p12

# ssl.keystore.password
# Варианты: строка-пароль keystore.
# Влияние: используется broker для чтения keystore; утечка опасна при доступе к файлу keystore.
ssl.keystore.password=changeit

# ssl.key.password
# Варианты: строка-пароль private key, может совпадать с keystore password.
# Влияние: нужен для доступа к закрытому ключу broker.
ssl.key.password=changeit

# ssl.truststore.location
# Варианты: путь к JKS или PKCS12 truststore.
# Влияние: содержит CA, которым broker доверяет; нужен для client cert auth и inter-broker SSL.
ssl.truststore.location=/etc/kafka/ssl/kafka.truststore.p12

# ssl.truststore.password
# Варианты: строка-пароль truststore.
# Влияние: неверный пароль не позволит broker прочитать доверенные CA.
ssl.truststore.password=changeit

# ssl.client.auth
# Варианты: none, requested, required.
# Влияние: none не требует client cert; requested запрашивает без обязательности; required включает mTLS и требует сертификат клиента.
ssl.client.auth=required
```

ACL включаются через authorizer:

```properties
# authorizer.class.name
# Варианты: org.apache.kafka.metadata.authorizer.StandardAuthorizer для KRaft.
# Влияние: включает проверку ACL; без authorizer ACL не будут ограничивать операции ожидаемым образом.
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer

# super.users
# Варианты: список principals через точку с запятой, например User:admin;User:CN=broker1.
# Влияние: эти principals обходят ACL checks; сюда добавляют admin и broker principals, чтобы не заблокировать управление.
super.users=User:admin;User:CN=broker1;User:CN=broker2;User:CN=broker3

# allow.everyone.if.no.acl.found
# Варианты: true, false.
# Влияние: false запрещает доступ без ACL и подходит для строгой безопасности; true удобен для миграции, но опасен в production.
allow.everyone.if.no.acl.found=false
```

Создание ACL для producer:

```bash
"$KAFKA_HOME/bin/kafka-acls.sh" \
  --bootstrap-server broker1.example.local:9093 \
  --command-config admin-client.properties \
  --add \
  --allow-principal "User:CN=producer" \
  --operation Write \
  --operation Describe \
  --topic secure-events
```

Создание ACL для consumer:

```bash
"$KAFKA_HOME/bin/kafka-acls.sh" \
  --bootstrap-server broker1.example.local:9093 \
  --command-config admin-client.properties \
  --add \
  --allow-principal "User:CN=consumer" \
  --operation Read \
  --operation Describe \
  --topic secure-events

"$KAFKA_HOME/bin/kafka-acls.sh" \
  --bootstrap-server broker1.example.local:9093 \
  --command-config admin-client.properties \
  --add \
  --allow-principal "User:CN=consumer" \
  --operation Read \
  --group secure-readers
```

## 11. Мониторинг и администрирование

### Теория

Мониторинг Kafka должен показывать состояние brokers, partitions, replication, consumers, storage и JVM. Consumer lag измеряет отставание конкретной consumer group, а не состояние всего кластера. JMX Exporter отдает broker/JVM metrics, Kafka Exporter удобен для lag и metadata, CLI-команды Kafka используются как контрольная диагностика.

Метрики Kafka разделяются по уровням:

```text
cluster level: active controller, broker count
topic level: bytes in/out, messages in
partition level: leader, ISR, under-replication
consumer group level: current offset, end offset, lag
host level: disk, network, CPU, page cache
JVM level: heap, GC, threads
```

Consumer lag нужно интерпретировать вместе с input rate и скоростью обработки. Lag растет при трех разных состояниях: producer пишет быстрее, consumer обрабатывает медленнее, consumer не может выполнить side effect во внешней системе. Для диагностики нужны logs consumer-приложения и метрики downstream-системы.

Kafka отдает метрики через JMX. Для Prometheus обычно используются:

- JMX Exporter - JVM и broker metrics;
- Kafka Exporter - consumer lag и metadata topics/partitions;
- Prometheus - сбор time series;
- Grafana - dashboards.

Мониторинг Kafka должен разделять состояние broker, состояние partitions и состояние consumers. Доступность exporter не означает, что Kafka работает корректно. `up=1` у JMX Exporter говорит только о том, что Prometheus смог опросить HTTP endpoint exporter.

Критичные сигналы broker:

- `UnderReplicatedPartitions` - followers отстали или недоступны;
- `OfflinePartitionsCount` - partitions без leader, чтение и запись невозможны;
- request latency - задержка обработки Produce/Fetch requests;
- network/request handler idle - запас потоков обработки;
- disk usage и disk IO latency;
- JVM heap, GC pauses и thread count.

Consumer lag отражает отставание конкретной consumer group от конца log. Он не всегда означает проблему Kafka. Часто lag растет из-за медленной БД, внешнего API, ошибок обработки, долгих batch operations или недостаточного числа consumers.

Метрики, которые проверяются в эксплуатации:

- broker availability;
- under-replicated partitions;
- offline partitions;
- active controller count;
- request latency;
- produce/fetch throughput;
- consumer lag;
- disk usage;
- JVM heap и GC.

Пример Prometheus scrape config:

```yaml
scrape_configs:
  # job_name
  # Варианты: произвольное имя Prometheus job.
  # Влияние: попадает в label job и используется в PromQL-фильтрах.
  - job_name: kafka-jmx
    static_configs:
      # targets
      # Варианты: список host:port endpoints JMX Exporter.
      # Влияние: если target недоступен, Prometheus выставит up=0 для этого endpoint.
      - targets:
          - localhost:7071

  # job_name
  # Варианты: отдельное имя job для Kafka Exporter.
  # Влияние: разделяет broker/JVM metrics и lag/topic metrics в запросах и dashboards.
  - job_name: kafka-exporter
    static_configs:
      # targets
      # Варианты: endpoints Kafka Exporter.
      # Влияние: exporter собирает consumer lag и metadata; недоступность target скрывает эти метрики из Prometheus.
      - targets:
          - localhost:9308
```

Проверка consumer group:

```bash
"$KAFKA_HOME/bin/kafka-consumer-groups.sh" \
  --bootstrap-server localhost:9092 \
  --describe \
  --group orders-reader
```

Типовой вывод:

```text
GROUP          TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID  HOST       CLIENT-ID
orders-reader orders  0          10              12              2    ...          /127.0.0.1 consumer-1
```

`LAG` содержит количество сообщений, которые group еще не прочитала.

## 12. Практические сценарии

### Теория

Практический Kafka-сценарий проектируется от события: выбираются topic, key, schema, retention, replication factor, producer, consumer groups и ACL. Producer отвечает за надежную публикацию события, consumer отвечает за идемпотентную обработку и корректный commit offset. Несколько consumer groups могут читать один topic независимо и выполнять разные действия.

Минимальная спецификация topic:

```text
topic: orders
event types: OrderCreated, OrderCancelled
key: order_id
partitions: 12
replication.factor: 3
min.insync.replicas: 2
retention.ms: 604800000
cleanup.policy: delete
producer acks: all
consumer commit: manual after side effect
schema: JSON/Avro/Protobuf with version
ACL: producer Write/Describe, consumer Read/Describe + group Read
```

Если topic хранит состояние, key становится обязательной частью модели. Если topic хранит историю фактов, key выбирается по требованию порядка и распределения нагрузки. Если downstream выполняет необратимый side effect, событие должно содержать `event_id`.

При проектировании Kafka-сценария сначала определяется поток событий, а не набор команд. Для каждого topic фиксируются тип события, key, consumers, retention, replication factor, требования к порядку, схема payload и ACL. Для каждого consumer фиксируются side effects, режим commit offset и способ идемпотентной повторной обработки.

Базовые проектные связки:

- надежность записи: `replication.factor + min.insync.replicas + producer acks + idempotence`;
- параллелизм чтения: `number of partitions + number of consumers in one group`;
- безопасность side effects: `manual commit + idempotent write + retry handling`;
- эксплуатационная устойчивость: `monitoring + capacity + tested recovery + controlled rebalancing`.

Типовые ошибки проектирования:

- использовать Kafka как синхронный RPC;
- рассчитывать на глобальный порядок во всем topic;
- делать auto commit до записи во внешнюю систему;
- не иметь `event_id` или другого idempotency key;
- создавать слишком мало partitions для высоконагруженного потока;
- создавать слишком много partitions без оценки нагрузки на brokers;
- хранить большие binary payloads вместо ссылок на object storage;
- использовать `acks=0` для критичных бизнес-событий.

### Система логирования

Поток:

```text
application -> log producer -> Kafka topic app-logs -> log consumer -> PostgreSQL/ClickHouse/Elasticsearch
```

Topic:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --create \
  --topic app-logs \
  --partitions 6 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config compression.type=lz4
```

Событие:

```json
{
  "ts": "2026-05-08T12:00:00Z",
  "service": "billing",
  "level": "ERROR",
  "message": "payment declined",
  "trace_id": "a1b2c3"
}
```

### Обработка заказов

Поток:

```text
order-service -> orders topic -> billing consumer group
                         ├── shipping consumer group
                         └── notifications consumer group
```

Каждая consumer group читает все события независимо. Внутри одной group partitions распределяются между consumers.

### IoT pipeline

Поток:

```text
sensors -> telemetry topic -> Kafka Streams aggregation -> telemetry-1m topic -> storage
```

Key выбирается по идентификатору устройства, чтобы события одного устройства попадали в одну partition и сохраняли порядок.
