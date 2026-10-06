# Эксперимент 1: падение одного брокера из трёх (`docker kill kafka1`)

Дата: 2026-10-06, 19:12–19:18 (MSK). Логи брокеров в UTC, то есть сдвинуты на −3 часа.

## Стенд
| Компонент | Значение |
|---|---|
| Kafka | `confluentinc/cp-kafka:7.9.0` (Kafka 3.9.0, сборка `7.9.0-ccs`). Тег 7.9.2 содержит `7.9.2-ccs`: номер патча Confluent не совпадает с апачевским |
| Кластер | 3 узла, KRaft combined mode (broker + controller), кворум из **3 голосов** |
| Топики | `topic_producer`, `topic_consumer`, `topic_consumer_2`: 1 партиция, RF=3, `min.insync.replicas=2`, `unclean.leader.election.enable=false` |
| Лидеры до эксперимента | `topic_producer` → 1, `topic_consumer` → 1, `topic_consumer_2` → 2 (задано через `--replica-assignment`) |
| Сервис | Spring Boot 4.1.1, Java 25, kafka-clients из BOM Boot 4.1.1, `acks=all` |
| bootstrap-servers | `kafka-old-1.decommissioned.test:9092, kafka-old-2.decommissioned.test:9092, localhost:9092 (kafka1), localhost:9093 (kafka2), localhost:9094 (kafka3)` |
| Нагрузка | по 1 msg/s в `topic_consumer` и `topic_consumer_2`, 1 req/s на `GET /api/v1/test` → `topic_producer` |

Роняли `kafka1`. Это третий хост в bootstrap-списке (как в инциденте), лидер `topic_producer` и `topic_consumer` и вдобавок координатор обеих consumer-групп.

## Хронология
| Время | Событие |
|---|---|
| 19:12:47 | старт нагрузки |
| 19:13:47 | `docker kill kafka1`: аварийное падение без controlled shutdown |
| 19:13:47.24 | клиенты видят разрыв: `Node 1 disconnected`, `Group coordinator localhost:9092 ... is unavailable`, `Connection to node 1 ... could not be established` |
| 19:13:54.38 | контроллер (kafka2): `Fencing broker 1 because its session has timed out`, `handleBrokerFenced: changing 53 partition(s)` |
| 19:13:54.4 | новые лидеры: `topic_producer` → 2, `topic_consumer` → 3, координаторы групп → kafka2. ISR сжат до `[2,3]` |
| 19:13:55.6 | отправки продюсера, ждавшие с момента падения, подтверждены (самая долгая `tookMs=7504`). Консюмеры продолжили читать |
| 19:17:02 | `docker start kafka1` |
| 19:18:37 | ISR всех топиков снова `[*,*,1]`, нагрузка остановлена |

## Результаты
```
CONSUMER topic_consumer:   sent=338 consumed=338 missing=0 duplicates=0
  longest pause: 8.0s (19:13:47 -> 19:13:55); max end-to-end lag=7.0s
CONSUMER topic_consumer_2: sent=338 consumed=338 missing=0 duplicates=0
  longest pause: 7.0s (19:13:47 -> 19:13:54); max end-to-end lag=6.0s
PRODUCER topic_producer:   requests=338 PRODUCED=338 PRODUCE_FAILED=0, все HTTP 200
  max latency во время отказа = 7.51s
```

## Выводы
1. **Инцидент не воспроизвёлся.** Продюсер и оба консюмера пережили падение брокера. Потерь и дублей нет, единственный эффект — пауза **~7–8 с**.
2. Пауза равна времени, за которое контроллер замечает падение и исключает брокер (fencing): `broker.session.timeout.ms` = 9 с по умолчанию. При `kill` controlled shutdown не выполняется, поэтому лидерство переходит только после истечения сессии брокера.
3. Две несуществующие записи в bootstrap-списке дают только `WARN ClientUtils: Couldn't resolve server ... DNS resolution failed` при создании каждого клиента и на работу не влияют. **Гипотеза «дело в bootstrap-servers» опровергнута.**
4. Падение лидера партиции и даже координатора группы само по себе не останавливает клиентов, пока в кластере есть **живое большинство кворума** (здесь 2 из 3) и живая реплика в ISR. Тогда контроллер переизбирает лидеров, и клиенты находят их по метаданным.
5. Значит, в проде должно было отличаться что-то на стороне кластера, а не клиентов. Главная оставшаяся гипотеза: **в кворуме контроллеров (или в ансамбле ZooKeeper) остались выведенные узлы**. Тогда после падения третьего брокера живых голосов 2 из 5, большинства нет и переизбирать лидеров некому. Подробности в [kafka-incident-analysis.md](kafka-incident-analysis.md).

## Следующий шаг (предложение)
Эксперимент 2: кворум из 5 голосов (`controller.quorum.voters` с 5 записями), при этом 2 узла не запущены, то есть «выведены». Затем ронять лидера `topic_producer` и `topic_consumer`. Ожидание: у партиций с лидером на упавшем узле лидер не переизберётся, у `topic_consumer_2` всё продолжит работать.

## Где лежат артефакты
- `logs/app.log` — лог сервиса (маркеры `CONSUMED`, `PRODUCED`, `PRODUCE_FAILED`)
- `logs/load/` — нагрузка: `sent-*.log`, `producer-*.log`, `http.log`
- `logs/brokers/kafka{1,2,3}.log` — логи брокеров (`docker logs -t`)
- `logs/snapshots/01..05-*.txt` — лидеры, ISR, состояние кворума и координаторы групп до, во время и после отказа
- `logs/experiment/events.log`, `logs/experiment/summary.txt` — хронология и сводка
