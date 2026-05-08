# Техническая структура Apache Kafka

Файл фиксирует состав руководства и порядок технических тем. Все практические подключения приложений, внешние интеграции, producers, consumers, HTTP API и обработчики событий выполняются на Python.

## 1. Базовая архитектура Kafka

Раздел вводит Kafka как распределенный журнал событий. Основные сущности: broker, cluster, topic, partition, offset, producer, consumer, consumer group, replica, leader, follower и ISR.

Техническая цепочка:

```text
producer application -> Kafka broker -> topic partition -> consumer application
```

Kafka принимает события от приложений, сохраняет их в append-only log и отдает разным группам consumers независимо друг от друга. Topic задает логическое имя потока, partition задает физический упорядоченный журнал, offset задает позицию записи внутри partition.

## 2. Standalone broker в KRaft mode

Раздел описывает установку Java, подготовку `/opt/kafka`, каталогов данных, форматирование KRaft storage и запуск одного broker. Один процесс выполняет роли `broker` и `controller`, поэтому подходит для базовой проверки протокола Kafka без распределенной части.

Проверяемые технические состояния:

- процесс Kafka запущен;
- listener `localhost:9092` принимает client connections;
- Admin API отвечает на `kafka-broker-api-versions.sh`;
- topic создается через `kafka-topics.sh`;
- producer записывает record, consumer читает record.

## 3. Topics, partitions и offsets

Раздел описывает маршрутизацию событий по partitions, роль key, порядок чтения и хранение offset. Partition является минимальной единицей параллелизма и порядка. Внутри одной partition Kafka сохраняет порядок records, между разными partitions глобального порядка нет.

Основные технические правила:

- один topic может иметь одну или несколько partitions;
- record с одинаковым key обычно попадает в одну partition;
- offset уникален только внутри конкретной partition;
- consumer group распределяет partitions между consumers;
- увеличение partitions повышает параллелизм, но меняет распределение key и усложняет порядок обработки.

## 4. Producers и consumers на Python

Раздел описывает Python producer, delivery callbacks, consumer polling loop, manual commit и обработку ошибок. Producer сериализует событие, выбирает partition, отправляет batch broker leader и получает подтверждение. Consumer читает records, выполняет обработку и фиксирует offset.

Критический порядок для at-least-once обработки:

```text
poll record -> process record -> write external state -> commit Kafka offset
```

Если offset фиксируется до внешней записи, при сбое возможна потеря обработки. Если offset фиксируется после внешней записи, при сбое возможен повтор, поэтому внешняя запись должна быть идемпотентной.

## 5. Multi-broker cluster

Раздел описывает несколько brokers, replication factor, leader/follower replicas, ISR и leader election. Replication factor определяет число копий partition. Leader принимает client I/O, followers копируют данные с leader. ISR содержит replicas, которые не отстают критически от leader.

Техническая модель отказа:

```text
producer -> leader replica -> follower replica
                         -> follower replica
```

При отказе leader Kafka выбирает нового leader из ISR. Если `min.insync.replicas` больше числа доступных ISR, запись с `acks=all` останавливается, чтобы не подтверждать события без достаточного числа копий.

## 6. Storage, retention и compaction

Раздел связывает Kafka topics с файлами на диске. Partition хранится как набор segment files. Retention удаляет старые segments по времени или размеру. Compaction сохраняет последнюю запись для каждого key и подходит для потоков состояния.

Основные параметры:

- `log.dirs` задает каталоги хранения partitions;
- `log.retention.hours` и `retention.ms` ограничивают время хранения;
- `log.retention.bytes` и `retention.bytes` ограничивают размер хранения;
- `cleanup.policy=delete` включает удаление старых segments;
- `cleanup.policy=compact` включает key-based compaction;
- `segment.bytes` и `segment.ms` влияют на размер и период закрытия segments.

## 7. Внешние подключения через Python и Kafka Connect

Раздел описывает обмен событиями между Kafka и PostgreSQL. Python-код используется для source/sink процессов, где логика преобразования и идемпотентность находятся в приложении. Kafka Connect рассматривается как инфраструктурный runtime для connector-процессов.

Поток source:

```text
PostgreSQL table -> Python source app -> Kafka topic
```

Поток sink:

```text
Kafka topic -> Python sink app -> PostgreSQL table
```

## 8. Stream processing через Python

Раздел описывает потоковую обработку: чтение input topic, вычисление агрегатов, запись output topic и хранение состояния. Для простых pipeline состояние может храниться в памяти или PostgreSQL. При stateful обработке состояние должно восстанавливаться после перезапуска процесса.

Типовая цепочка:

```text
input topic -> Python processor -> state store -> output topic
```

## 9. TLS, ACL и контроль доступа

Раздел описывает защиту сетевого канала и авторизацию операций. TLS шифрует соединение и позволяет проверять сертификаты. ACL ограничивает операции principals над resources: topics, groups, cluster.

Порядок проверки запроса:

```text
network connection -> TLS handshake -> principal identity -> ACL check -> Kafka operation
```

Без ACL любой подключившийся клиент может выполнять операции, разрешенные listener и broker-настройками. С ACL запись, чтение, создание topics и работа consumer groups требуют явных прав.

## 10. Monitoring и диагностика

Раздел описывает проверку процесса, listener, Kafka protocol, состояния topics, consumer lag, ISR и дискового хранения. Метрики Kafka снимаются через JMX exporter, затем собираются Prometheus и отображаются в Grafana.

Минимальная диагностическая цепочка:

```text
systemd status -> listening ports -> Kafka API -> topic metadata -> consumer lag -> broker logs
```

## 11. Прикладные event-driven потоки

Раздел объединяет Kafka topics, Python producers, Python consumers, consumer groups, offsets, PostgreSQL и monitoring. Одна запись в Kafka может запускать несколько независимых процессов: биллинг, доставку, уведомления, аудит и аналитическую обработку.

Базовая схема:

```text
HTTP API -> orders topic -> billing consumer -> PostgreSQL
                     |
                     +-> shipping consumer -> PostgreSQL
                     |
                     +-> notifications consumer -> external API
```

