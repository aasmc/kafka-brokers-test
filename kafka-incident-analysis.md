# Разбор инцидента: отвал продюсера topic_producer и консюмера topic_consumer

## Исходные данные
- Сервис: Java 25, Spring Boot 4.1.0, Spring Kafka (kafka-clients 4.x).
- Три топика, в каждом по одной партиции: `topic_producer` (продюсер), `topic_consumer` (консюмер), `topic_consumer_2` (консюмер).
- В `bootstrap-servers` указано 5 хостов. Первые 2 выведены из эксплуатации несколько недель назад. Данные и реплики с них **перенесли на остальные брокеры**, а конфиги клиентов не поправили, поэтому в логах были ворнинги.
- Упал третий брокер. Живыми остались 4-й и 5-й.
- Результат: продюсер `topic_producer` и консюмер `topic_consumer` перестали работать, консюмер `topic_consumer_2` продолжил.

## Bootstrap-список не причина
`bootstrap.servers` нужен клиенту только для первого запроса метаданных. Клиент перебирает адреса, находит живой хост (4-й или 5-й) и получает от него полную карту кластера: брокеры, лидеров партиций, координаторов. Дальше он работает с брокерами из метаданных и к bootstrap-списку не обращается.

Значит, причина в другом: у двух топиков не нашлось живого **лидера партиции** (а у консюмера ещё **group coordinator**), а у `topic_consumer_2` он был.

После вывода двух серверов в кластере осталось три брокера (3, 4, 5). При RF=3 и исправном кластере падение одного из них должно пройти незаметно. Раз не прошло, остаются гипотезы ниже.

## Гипотеза 1 (наиболее вероятная): кворум контроллеров по-прежнему считает 5 участников
Данные перенесли, но выведенные серверы могли остаться в составе кворума. Это голосующие контроллеры KRaft (`controller.quorum.voters`) или узлы ZooKeeper, если они стояли на тех же машинах.

Как развивались события:
1. В кворуме 5 голосов, 2 из них мертвы уже несколько недель. Большинство (3 из 5) держалось только благодаря брокеру 3. Ворнинги в логах эту проблему не выдавали.
2. Брокер 3 упал. Живых голосов осталось 2 из 5, большинства нет, **активного контроллера нет**.
3. Без контроллера не проходят выборы лидеров партиций и не меняется ISR.
4. Партиции, у которых лидер был на брокере 3, остались без лидера: `topic_producer`, `topic_consumer` и, возможно, партиция `__consumer_offsets` с координатором их группы.
5. Партиции, у которых лидер и так был на брокере 4 или 5, продолжили обслуживать клиентов. Поэтому работает `topic_consumer_2`.

Эта гипотеза точно объясняет картину «два топика упали, один нет» и не требует проблем с размещением реплик.

Как проверить:
```bash
kafka-metadata-quorum.sh --bootstrap-server <live> describe --status       # CurrentVoters, LeaderId
kafka-metadata-quorum.sh --bootstrap-server <live> describe --replication
```
Если в `CurrentVoters` пять id, гипотеза подтверждена. Для ZooKeeper смотрите `server.N` в `zoo.cfg` и `echo mntr | nc <zk> 2181`. В логах брокеров за время инцидента ищите признаки отсутствия активного контроллера.

## Гипотеза 2: настройки конкретных топиков
- **RF=1** у `topic_producer` и `topic_consumer` с единственной репликой на брокере 3. После его падения партиция offline (`leader = -1`).
- **`min.insync.replicas` равен RF** (например, RF=2 и min.isr=2). При `acks=all` (дефолт с Kafka 3.0) продюсер получает `NotEnoughReplicasException`, хотя чтение продолжает работать.
- Проверьте также служебные топики `__consumer_offsets` и `__transaction_state`. Координатор группы — это лидер партиции `abs(groupId.hashCode()) % offsets.topic.num.partitions` (по умолчанию 50). У разных групп разные координаторы.

Как проверить:
```bash
kafka-topics.sh --bootstrap-server <live> --describe --topic topic_producer
kafka-topics.sh --bootstrap-server <live> --describe --topic topic_consumer
kafka-topics.sh --bootstrap-server <live> --describe --topic topic_consumer_2
kafka-topics.sh --bootstrap-server <live> --describe --topic __consumer_offsets
kafka-topics.sh --bootstrap-server <live> --describe --topic __transaction_state
kafka-configs.sh --bootstrap-server <live> --describe --entity-type topics --entity-name topic_producer
kafka-consumer-groups.sh --bootstrap-server <live> --describe --group <group> --state
```

## Гипотеза 3: консюмер упал вслед за продюсером
Если обработчик `topic_consumer` пишет в `topic_producer` (цепочка «прочитал → обработал → отправил», тем более в транзакции через `KafkaTransactionManager`), то зависший `send()` блокирует listener. Дальше превышается `max.poll.interval.ms`, начинается ребаланс, транзакция откатывается и сообщения читаются заново. Со стороны это выглядит как отвал консюмера, хотя его партиция исправна. `topic_consumer_2`, видимо, ничего не отправляет, поэтому продолжил работать.

## Что нужно для окончательного вывода
- Кластер на KRaft или ZooKeeper, и сколько участников в кворуме.
- Вывод `kafka-topics.sh --describe` по трём рабочим и двум служебным топикам.
- Пишет ли обработчик `topic_consumer` в `topic_producer` и используются ли транзакции (`transaction-id-prefix`).
- Ошибки клиента за время инцидента: `LEADER_NOT_AVAILABLE`, `NOT_LEADER_OR_FOLLOWER`, `COORDINATOR_NOT_AVAILABLE`, `NotEnoughReplicas`, `not present in metadata`.

## Рекомендации
- Удалить выведенные узлы из кворума: `kafka-metadata-quorum.sh remove-controller` для динамического кворума KRaft, правка `controller.quorum.voters` с перезапуском для статического, реконфигурация ансамбля для ZooKeeper. Разрегистрировать выведенные брокеры (`kafka-cluster.sh unregister`).
- Проверить RF и `min.insync.replicas` у всех топиков, включая служебные. Для 3 брокеров разумно RF=3 и min.isr=2.
- Мониторить `ActiveControllerCount`, `OfflinePartitionsCount`, `UnderReplicatedPartitions`, `UnderMinIsrPartitionCount`.
- Удалить мёртвые хосты из `bootstrap-servers`. Это гигиена, к инциденту отношения не имеет.

## Проверка на стенде (2026-10-06)
Подробности в [EXPERIMENT-RESULTS.md](EXPERIMENT-RESULTS.md).
- **Эксперимент 1** (кворум 3/3, падение одного брокера): инцидент не воспроизвёлся, пауза ~8 с. Версия про `bootstrap-servers` опровергнута.
- **Эксперимент 2** (в кворуме 5 голосов, 2 выведенных узла, падение одного живого брокера): инцидент воспроизвёлся. Продюсер получал 100% ошибок, консюмер с координатором на упавшем брокере бесконечно искал координатора. Консюмер второго топика не писал в лог ни одной ошибки и показывал лаг 0, но и сообщений не получал: high watermark заморожен, потому что ISR нельзя сжать без контроллера. **Гипотеза 1 подтверждена.**
