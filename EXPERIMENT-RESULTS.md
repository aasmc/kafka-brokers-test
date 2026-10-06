# Результаты экспериментов

Дата: 2026-10-06 (MSK). Логи брокеров в UTC, то есть сдвинуты на −3 часа.

## Общий стенд
| Компонент | Значение |
|---|---|
| Kafka | `confluentinc/cp-kafka:7.9.0` (Kafka 3.9.0, сборка `7.9.0-ccs`). Тег 7.9.2 содержит `7.9.2-ccs`: номер патча Confluent не совпадает с апачевским |
| Узлы | 3 живых узла `kafka1..3`, KRaft combined mode (broker + controller) |
| Топики | `topic_producer`, `topic_consumer`, `topic_consumer_2`: 1 партиция, RF=3, `min.insync.replicas=2`, `unclean.leader.election.enable=false` |
| Лидеры | `topic_producer` → 1, `topic_consumer` → 1, `topic_consumer_2` → 2 (задано через `--replica-assignment`) |
| Сервис | Spring Boot 4.1.1, Java 25, `acks=all`. Таймаут ожидания подтверждения отправки в эндпоинте 10 с |
| bootstrap-servers | 2 несуществующих хоста + `localhost:9092` (kafka1), `localhost:9093` (kafka2), `localhost:9094` (kafka3) |
| Нагрузка | по 1 msg/s в `topic_consumer` и `topic_consumer_2` (console producer, `acks=all`), 1 req/s на `GET /api/v1/test` → `topic_producer` |
| Отказ | `docker kill kafka1`: аварийное падение третьего хоста из bootstrap-списка |

Профили экспериментов лежат в `experiments/exp*.env`, Spring-профили в `app/src/main/resources/application-exp*.yml`.

---

## Эксперимент 1: кворум 3 из 3 (`EXP=exp1`)
Кворум KRaft из 3 голосов, все три узла живы. Координаторы обеих групп на kafka1.

### Хронология
| Время | Событие |
|---|---|
| 19:13:47 | `docker kill kafka1`. Клиенты сразу видят `Node 1 disconnected`, `Group coordinator localhost:9092 ... is unavailable` |
| 19:13:54.38 | контроллер (kafka2): `Fencing broker 1 because its session has timed out`, `handleBrokerFenced: changing 53 partition(s)` |
| 19:13:54.4 | новые лидеры: `topic_producer` → 2, `topic_consumer` → 3, координаторы групп → kafka2. ISR сжат до `[2,3]` |
| 19:13:55.6 | ожидавшие отправки подтверждены (самая долгая `tookMs=7504`), консюмеры продолжили чтение |
| 19:17:02 | `docker start kafka1`, ISR восстановился |

### Результаты
```
CONSUMER topic_consumer:   sent=338 consumed=338 missing=0 duplicates=0, longest pause 8s
CONSUMER topic_consumer_2: sent=338 consumed=338 missing=0 duplicates=0, longest pause 7s
PRODUCER topic_producer:   338 запросов, все HTTP 200, max latency 7.5s
```
**Вывод:** инцидент не воспроизвёлся. Единственный эффект — пауза ~7–8 с, пока контроллер ждёт истечения сессии брокера (`broker.session.timeout.ms` = 9 с). Записи в bootstrap про несуществующие хосты дают только `WARN ClientUtils: Couldn't resolve server ...`.

---

## Эксперимент 2: в кворуме 5 голосов, 2 из них выведены (`EXP=exp2`)
`controller.quorum.voters` содержит 5 голосов: `1..3` (живые) и `4@kafka-old-1`, `5@kafka-old-2` («выведенные» узлы, которых нет). Реплик на выведенных узлах нет. Кластер работает, потому что большинство 3 из 5 есть. Координатор группы consumer-1 размещён на kafka1, consumer-2 — на kafka2 (`scripts/pick-groups.py`).

### Хронология
| Время | Событие |
|---|---|
| 19:30:04 | `docker kill kafka1`. Живых голосов 2 из 5, большинства нет |
| 19:30:07 | kafka2: `Become candidate due to fetch timeout`, затем `In the new epoch N, the leader is (none)`. Выборы контроллера идут по кругу и не завершаются |
| 19:30:04–19:33:04 | метаданные заморожены: `topic_producer Leader: 1 Isr: 1,2,3`, `topic_consumer Leader: 1 Isr: 1,3,2`. Брокер 1 не исключён (некому), лидеры не переизбираются, ISR не сжимается. `kafka-metadata-quorum describe` отваливается по таймауту |
| 19:33:04 | `docker start kafka1`, кворум 3 из 5 восстановлен |
| 19:33:07 | `topic_consumer_2`: разом пришли все сообщения, накопленные за время отказа |
| 19:33:16 | `topic_producer` снова пишет, `topic_consumer` снова читает |

### Результаты
```
PRODUCER topic_producer:  во время отказа 174 из 174 запросов → HTTP 503
                          (TimeoutException на 10 c; через 120 c - KafkaProducerException: Failed to send)
                          простой 19:30:04 -> 19:33:16 (~3 мин 12 с)
CONSUMER topic_consumer:  0 сообщений за время отказа, пауза 193 c
                          в логе 253 WARN + бесконечное "Group coordinator localhost:9092 is unavailable"
                          (координатор группы на мёртвом брокере)
CONSUMER topic_consumer_2: 0 сообщений за время отказа, пауза 184 c,
                          НО в логе приложения ни одного WARN/ERROR от этого консюмера,
                          группа Stable, координатор kafka2 жив, LAG = 0
```
Состояние группы `topic_consumer_2` в середине отказа (`logs/exp2/manual-groups-during-down.txt`):
```
GROUP                           COORDINATOR (ID)   STATE   #MEMBERS
kafka-test-service-consumer-2-1 kafka2:29092 (2)   Stable  1
TOPIC            PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
topic_consumer_2 0          58              58              0
```

Что происходило с нагрузочными продюсерами:
- `topic_consumer` (лидер на мёртвом брокере): 117 записей не дошли (`Expiring 117 record(s) ... since batch creation`). Это потери на стороне отправителя, консюмер их и не мог получить.
- `topic_consumer_2` (лидер kafka2 жив): 164 ошибки `REQUEST_TIMED_OUT / Disconnected from node 2 due to timeout`, но после восстановления консюмер получил **все** 319 сообщений. Лидер записывал их в лог, но не мог подтвердить: в ISR остался мёртвый брокер 1, а сжать ISR без контроллера нельзя. High watermark стоял на месте, и консюмер не видел новых записей. Отправитель получил ошибку, хотя данные сохранились. При ретраях на уровне приложения это дало бы дубли.

### Выводы
1. **Гипотеза подтверждена для продюсера и `topic_consumer`.** Если в кворуме остались выведенные узлы, падение одного живого брокера лишает кластер контроллера. Лидеры партиций с упавшего брокера не переизбираются, координаторы групп с него не переезжают. Клиенты видят ровно то, что было в проде: продюсер не может отправить, консюмер бесконечно ищет координатора. Bootstrap-список ни при чём.
2. **`topic_consumer_2` «продолжил работать» только внешне.** Процесс жив, ошибок в логе нет, группа `Stable`, лаг 0 (лаг считается от high watermark, а он заморожен). Но новых сообщений консюмер не получал, потому что запись в его топик тоже встала. Так, скорее всего, было и в проде: консюмер считали рабочим по отсутствию ошибок и алертов.
3. **Чтобы убедиться, проверьте в проде** за время инцидента, были ли у `topic_consumer_2` реально обработанные сообщения (бизнес-метрики, offset’ы коммитов), а не только отсутствие ошибок. Если сообщения были, значит, ISR его партиции не содержал упавший брокер, и картина сложнее.
4. Сравнение экспериментов: при одинаковых клиентах, топиках и bootstrap-списке исход определяет только состав кворума (3/3 против 3/5).

### Рекомендации
- Удалить выведенные узлы из кворума контроллеров (`kafka-metadata-quorum remove-controller` для динамического кворума, правка `controller.quorum.voters` с rolling restart для статического). Для ZooKeeper — убрать их из ансамбля.
- Мониторить `ActiveControllerCount` (сумма по кластеру должна быть 1), `kafka.raft` метрики кворума, `OfflinePartitionsCount`, `UnderMinIsrPartitionCount`.
- Мониторить консюмеров не только по ошибкам и лагу, но и по скорости обработки и времени с последнего сообщения: при замороженном high watermark лаг равен 0.

## Артефакты
Логи каждого эксперимента лежат в `logs/<exp>/`:
- `app.log` — лог сервиса (маркеры `CONSUMED`, `PRODUCED`, `PRODUCE_FAILED`)
- `load/` — нагрузка: `sent-*.log`, `producer-*.log`, `http.log`
- `brokers/kafka{1,2,3}.log` — логи брокеров
- `snapshots/01..05-*.txt` — лидеры, ISR, кворум и координаторы групп до, во время и после отказа. В exp2 часть админ-команд во время отказа отваливается по таймауту, это тоже результат
- `experiment/events.log`, `experiment/summary.txt` — хронология и сводка
- `groups.env`, `consumer-offsets-describe.txt` — выбранные id групп и размещение `__consumer_offsets`
