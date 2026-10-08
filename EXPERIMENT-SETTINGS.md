# Настройки стенда: брокеры, топики, консюмеры, продюсер (exp1 и exp2)

Источники: `docker-compose.yml`, `docker-compose.exp2-quorum5.yml`, `experiments/exp*.env`,
`scripts/create-topics.sh`, `scripts/load.sh`, `app/src/main/resources/application*.yml`, код сервиса,
а также фактические значения из логов запусков (`logs/exp*/app.log` — дампы `ProducerConfig`/`ConsumerConfig`,
`logs/exp*/brokers/kafka1.log` — дамп `KafkaConfig`, `logs/exp*/snapshots/01-before.txt`).

**Коротко:** фактор репликации всех топиков в обоих экспериментах — **3** (`min.insync.replicas=2`).
Эксперименты отличаются только кворумом KRaft (3 голоса против 5), адресами двух «мёртвых» хостов
в bootstrap-списке сервиса и id групп консюмеров. Всё остальное одинаково.

---

## 1. Различия между экспериментами

| Параметр | exp1 | exp2 |
|---|---|---|
| Compose-файлы | `docker-compose.yml` | `docker-compose.yml` + `docker-compose.exp2-quorum5.yml` |
| `controller.quorum.voters` | `1@kafka1:29093,2@kafka2:29093,3@kafka3:29093` | `1@kafka1:29093,2@kafka2:29093,3@kafka3:29093,4@kafka-old-1:29093,5@kafka-old-2:29093` |
| Голосов в кворуме / живых / нужно для большинства | 3 / 3 / 2 | 5 / 3 / 3 |
| Spring-профиль | `exp1` | `exp2` |
| `bootstrap.servers` сервиса (продюсер и оба консюмера) | `kafka-old-1.decommissioned.test:9092, kafka-old-2.decommissioned.test:9092, localhost:9092, localhost:9093, localhost:9094` | `kafka-old-1:9092, kafka-old-2:9092, localhost:9092, localhost:9093, localhost:9094` |
| `group.id` consumer-1 (`topic_consumer`) | `kafka-test-service-consumer-1` | `kafka-test-service-consumer-1-2` |
| `group.id` consumer-2 (`topic_consumer_2`) | `kafka-test-service-consumer-2` | `kafka-test-service-consumer-2-1` |

Id групп подбираются `scripts/pick-groups.py` так, чтобы координатор consumer-1 был на брокере 1, а consumer-2 —
на брокере 2 (`GROUP1_COORDINATOR=1`, `GROUP2_COORDINATOR=2`); разные значения id — следствие разного
размещения лидеров `__consumer_offsets`, а не разной настройки.

Сценарий в обоих профилях одинаковый: `TARGET=kafka1`, `MODE=kill`, фазы `BASELINE=60` с, `DOWN=180` с,
`RECOVERY=90` с.

---

## 2. Брокеры (одинаково в exp1 и exp2, кроме кворума)

Образ `confluentinc/cp-kafka:7.9.0` (Kafka `7.9.0-ccs` ≈ Apache Kafka 3.9.0), `inter.broker.protocol.version=3.9-IV0`.
Три узла `kafka1`, `kafka2`, `kafka3`, KRaft combined mode (`process.roles=broker,controller`), `restart: "no"`.

### Заданные явно (`x-kafka-env` в `docker-compose.yml`)

| Настройка | Значение |
|---|---|
| `CLUSTER_ID` | `MkU3OEVBNTcwNTJENDM2Qk` |
| `node.id` | 1 / 2 / 3 |
| `process.roles` | `broker,controller` |
| `controller.quorum.voters` | см. раздел 1 |
| `controller.listener.names` | `CONTROLLER` |
| `inter.broker.listener.name` | `INTERNAL` |
| `listener.security.protocol.map` | `INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT` |
| `listeners` | `INTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,EXTERNAL://0.0.0.0:9092` |
| `advertised.listeners` | `INTERNAL://kafkaN:29092,CONTROLLER://kafkaN:29093,EXTERNAL://localhost:909{2,3,4}` |
| Проброс портов на хост | kafka1 → 9092, kafka2 → 9093, kafka3 → 9094 |
| `log.dirs` | `/var/lib/kafka/data` |
| `auto.create.topics.enable` | `false` |
| `default.replication.factor` | `3` |
| `min.insync.replicas` | `2` |
| `offsets.topic.replication.factor` | `3` |
| `transaction.state.log.replication.factor` | `3` |
| `transaction.state.log.min.isr` | `2` |
| `group.initial.rebalance.delay.ms` | `0` |
| `unclean.leader.election.enable` | `false` |

### Значимые для экспериментов значения по умолчанию (из дампа `KafkaConfig`)

| Настройка | Значение |
|---|---|
| `num.partitions` | `1` |
| `offsets.topic.num.partitions` | `50` |
| `replica.lag.time.max.ms` | `30000` |
| `broker.heartbeat.interval.ms` | `2000` |
| `broker.session.timeout.ms` | `9000` |
| `controller.quorum.election.timeout.ms` | `1000` |
| `controller.quorum.election.backoff.max.ms` | `1000` |
| `controller.quorum.fetch.timeout.ms` | `2000` |
| `controller.quorum.request.timeout.ms` | `2000` |
| `controller.quorum.retry.backoff.ms` | `20` |
| `group.coordinator.rebalance.protocols` | `[classic]` |
| `group.min.session.timeout.ms` / `group.max.session.timeout.ms` | `6000` / `1800000` |
| `auto.leader.rebalance.enable` | `true` |
| `leader.imbalance.check.interval.seconds` | `300` |
| `log.retention.hours` | `168` |

Плюс контейнер `kafka-tools` (тот же образ): не брокер, создаёт топики и запускает console producer нагрузки.

---

## 3. Топики (одинаково в exp1 и exp2)

Создаются `scripts/create-topics.sh` с явным `--replica-assignment` (первая реплика — предпочтительный лидер)
и `--config min.insync.replicas=2`.

| Топик | Партиций | RF | Реплики | Лидер | `min.insync.replicas` | `unclean.leader.election.enable` |
|---|---|---|---|---|---|---|
| `topic_producer` | 1 | **3** | 1,2,3 | kafka1 | 2 | false |
| `topic_consumer` | 1 | **3** | 1,3,2 | kafka1 | 2 | false |
| `topic_consumer_2` | 1 | **3** | 2,3,1 | kafka2 | 2 | false |
| `__consumer_offsets` | 50 | **3** | распределены по 1–3 | разные | 2 | false |

`__consumer_offsets` дополнительно: `cleanup.policy=compact`, `compression.type=producer`, `segment.bytes=104857600`.
Создаётся заранее прогревочным консюмером группы `warmup`.

RF=3 подтверждён снимками `logs/exp1/snapshots/01-before.txt` и `logs/exp2/snapshots/01-before.txt`.

---

## 4. Продюсер сервиса (`KafkaTemplate`, Spring Boot 4.1.1, kafka-clients 4.2.1)

Пишет в `topic_producer` по `GET /api/v1/test`, ждёт подтверждения не дольше 10 с (`SEND_TIMEOUT_SECONDS`),
иначе отвечает 503.

### Заданные явно (`application.yml`)

| Настройка | Значение |
|---|---|
| `acks` | `all` (в дампе `-1`) |
| `key.serializer` / `value.serializer` | `StringSerializer` |
| `bootstrap.servers` | см. раздел 1 |

### Фактические значения по умолчанию (дамп `ProducerConfig`, одинаковы в exp1 и exp2)

| Настройка | Значение |
|---|---|
| `client.id` | `kafka-test-service-producer-1` |
| `enable.idempotence` | `true` |
| `retries` | `2147483647` |
| `max.in.flight.requests.per.connection` | `5` |
| `delivery.timeout.ms` | `120000` |
| `request.timeout.ms` | `30000` |
| `max.block.ms` | `60000` |
| `linger.ms` | `5` |
| `batch.size` | `16384` |
| `buffer.memory` | `33554432` |
| `compression.type` | `none` |
| `client.dns.lookup` | `use_all_dns_ips` |
| `metadata.max.age.ms` | `300000` |
| `metadata.max.idle.ms` | `300000` |
| `metadata.recovery.strategy` | `rebootstrap` |
| `metadata.recovery.rebootstrap.trigger.ms` | `300000` |
| `reconnect.backoff.ms` / `reconnect.backoff.max.ms` | `50` / `1000` |
| `retry.backoff.ms` / `retry.backoff.max.ms` | `100` / `1000` |
| `socket.connection.setup.timeout.ms` / `.max.ms` | `10000` / `30000` |
| `connections.max.idle.ms` | `540000` |
| `partitioner.adaptive.partitioning.enable` | `true` |
| `security.protocol` | `PLAINTEXT` |
| `transactional.id` | `null` |

---

## 5. Консюмеры сервиса (`@KafkaListener`, kafka-clients 4.2.1)

| Listener | Топик | `client.id` (exp1 / exp2) |
|---|---|---|
| `consumer-1` | `topic_consumer` | `consumer-kafka-test-service-consumer-1-2` / `consumer-kafka-test-service-consumer-1-2-2` |
| `consumer-2` | `topic_consumer_2` | `consumer-kafka-test-service-consumer-2-1` / `consumer-kafka-test-service-consumer-2-1-1` |

`group.id` — см. раздел 1. Настройки обоих консюмеров одинаковы, отличаются только `group.id`/`client.id`.

### Заданные явно (`application.yml`)

| Настройка | Значение |
|---|---|
| `auto.offset.reset` | `earliest` |
| `key.deserializer` / `value.deserializer` | `StringDeserializer` |
| `bootstrap.servers` | см. раздел 1 |

### Фактические значения по умолчанию (дамп `ConsumerConfig`)

| Настройка | Значение |
|---|---|
| `enable.auto.commit` | `false` (выставляет Spring Kafka; коммит делает контейнер, AckMode по умолчанию `BATCH`) |
| `group.protocol` | `classic` |
| `partition.assignment.strategy` | `RangeAssignor, CooperativeStickyAssignor` |
| `session.timeout.ms` | `45000` |
| `heartbeat.interval.ms` | `3000` |
| `max.poll.interval.ms` | `300000` |
| `max.poll.records` | `500` |
| `default.api.timeout.ms` | `60000` |
| `request.timeout.ms` | `30000` |
| `fetch.min.bytes` / `fetch.max.wait.ms` | `1` / `500` |
| `fetch.max.bytes` / `max.partition.fetch.bytes` | `52428800` / `1048576` |
| `isolation.level` | `read_uncommitted` |
| `allow.auto.create.topics` | `true` (на брокере автосоздание выключено) |
| `group.instance.id` | `null` (динамическое членство) |
| `client.dns.lookup` | `use_all_dns_ips` |
| `metadata.max.age.ms` | `300000` |
| `metadata.recovery.strategy` | `rebootstrap` |
| `metadata.recovery.rebootstrap.trigger.ms` | `300000` |
| `reconnect.backoff.ms` / `reconnect.backoff.max.ms` | `50` / `1000` |
| `retry.backoff.ms` / `retry.backoff.max.ms` | `100` / `1000` |
| `socket.connection.setup.timeout.ms` / `.max.ms` | `10000` / `30000` |
| `connections.max.idle.ms` | `540000` |
| `security.protocol` | `PLAINTEXT` |

---

## 6. Нагрузка (`scripts/load.sh`, одинаково в exp1 и exp2)

| Источник | Куда | Темп | Настройки |
|---|---|---|---|
| `kafka-console-producer` в `kafka-tools` | `topic_consumer`, `topic_consumer_2` | 1 сообщение/с в каждый | `bootstrap.servers=kafka1:29092,kafka2:29092,kafka3:29092` (только живые брокеры), `acks=all`, `linger.ms=0`, остальное по умолчанию |
| `curl` → `GET /api/v1/test` | `topic_producer` (через сервис) | 1 запрос/с | `--max-time 20` |

Продолжительность `DURATION` по умолчанию 600 с; `experiment.sh` ограничивает её фазами профиля.
