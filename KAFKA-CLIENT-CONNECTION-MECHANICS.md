# Как Kafka-клиент подключается к брокерам и что делает с недоступными

Разбор для **kafka-clients 4.2.1**. Это версия, которую подтягивает Spring Boot 4.1.1 в нашем стенде
(`BOOT-INF/lib/kafka-clients-4.2.1.jar` в собранном jar сервиса). В проде Spring Boot 4.1.0, и версию
kafka-clients там нужно проверить отдельно (`mvn dependency:tree -Dincludes=org.apache.kafka`).

**Как проверялось:**
- Каждое утверждение ниже подтверждено ссылкой на строки исходников тега [`4.2.1`](https://github.com/apache/kafka/tree/4.2.1)
  или на официальную документацию. Исходники из `kafka-clients-4.2.1-sources.jar` (Maven Central) побайтно
  совпадают с тегом на GitHub, поэтому номера строк в ссылках точные.
- Ключевые выводы дополнительно проверены пробой: настоящий `KafkaProducer` 4.2.1 и «мёртвые» адреса трёх видов
  (раздел 7, там же код пробы и лог).
- Там, где поведение зависит не от Kafka, а от ОС или сети, это сказано прямо.

Сокращения для ссылок:
- **SRC** = `https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka`
- документация: [producer configs 4.2](https://kafka.apache.org/42/configuration/producer-configs/),
  [consumer configs 4.2](https://kafka.apache.org/42/configuration/consumer-configs/)

---

## Коротко

| Что с адресом | Что делает клиент | Сколько теряет | Что в логе |
|---|---|---|---|
| Имя не резолвится в DNS | выбрасывает адрес ещё при создании клиента | 0 | `WARN ClientUtils: Couldn't resolve server … DNS resolution failed` |
| Хост отвечает отказом (порт закрыт, RST) | попытка проваливается сразу, клиент идёт к следующему адресу | миллисекунды | `WARN NetworkClient: Connection to node -N (…) could not be established. Node may not be available.` |
| Пакеты молча теряются (хост выключен, firewall DROP) | ждёт `socket.connection.setup.timeout.ms`, **в это время другие адреса не пробует** | 8–12 с на первой попытке, дальше растёт до 24–30 с | `INFO NetworkClient: Disconnecting from node -N due to socket connection setup timeout. The timeout value is … ms.` |

Главные настройки:
- `socket.connection.setup.timeout.ms` — по умолчанию 10 000;
- `socket.connection.setup.timeout.max.ms` — по умолчанию 30 000;
- `reconnect.backoff.ms` — по умолчанию 50;
- `reconnect.backoff.max.ms` — по умолчанию 1000;
- для продюсера ещё `max.block.ms` — по умолчанию 60 000: сколько `send()` ждёт метаданные.

---

## 1. Разбор `bootstrap.servers` при создании клиента

1. Продюсер и оба варианта консюмера (classic и новый async) при создании вызывают одну и ту же функцию
   `ClientUtils.parseAndValidateAddresses(config)` и передают результат в `metadata.bootstrap(...)`:
   [KafkaProducer L440–L451](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java#L440-L451),
   [ClassicKafkaConsumer L191–L192](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ClassicKafkaConsumer.java#L191-L192),
   [AsyncKafkaConsumer L416–L417](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AsyncKafkaConsumer.java#L416-L417).
   Значит, всё описанное ниже одинаково для продюсера и консюмера.

2. Для каждого адреса создаётся `InetSocketAddress(host, port)`. Если имя не резолвится, адрес **пропускается**
   с WARN, ожидания нет:
   [ClientUtils L87–L94](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/ClientUtils.java#L87-L94).
   - Это ветка по умолчанию: `client.dns.lookup` по умолчанию `use_all_dns_ips`
     ([ProducerConfig L382–L386](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java#L382-L386),
     [ConsumerConfig L422–L426](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerConfig.java#L422-L426)).
   - При `resolve_canonical_bootstrap_servers_only` работает другая ветка, с канонизацией имён:
     [ClientUtils L76–L86](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/ClientUtils.java#L76-L86).

3. Если не зарезолвился **ни один** адрес, создание клиента падает с
   `ConfigException("No resolvable bootstrap urls given in bootstrap.servers")`:
   [ClientUtils L103–L104](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/ClientUtils.java#L103-L104).

4. Оставшиеся адреса превращаются во «временные узлы» с **отрицательными id**: -1, -2, -3… в порядке списка:
   [MetadataSnapshot L234–L244](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/MetadataSnapshot.java#L234-L244).
   Поэтому в логах встречаются `Node -1 disconnected` и `id: -2`.

> Подтверждение на нашем стенде: несуществующие `kafka-old-1/2…` из bootstrap-списка дают ровно этот WARN при
> создании каждого клиента (`logs/exp1/app.log`, `logs/exp2/app.log`) и больше никак не участвуют в работе.

---

## 2. Как выбирается адрес для первого запроса метаданных

Список адресов для подключения клиент берёт из текущих метаданных
([NetworkClient L1197–L1199](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java#L1197-L1199)). До первого ответа брокера это
временные узлы из `bootstrap.servers`. Узел выбирает `leastLoadedNode`:
[NetworkClient L759–L822](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java#L759-L822).

1. **Обход начинается со случайной позиции** в списке:
   [L771](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java#L771).
   Поэтому порядок адресов в `bootstrap.servers` не означает порядок попыток. В пробе (раздел 7) первым один раз
   выбрался `-2`, другой раз `-1`.

2. Приоритеты выбора ([L807–L821](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java#L807-L821), комментарий в коде:
   *«We prefer established connections if possible. Otherwise, we will wait for connections which are being
   established before connecting to new nodes»*):
   1. узел с уже установленным соединением ([L782–L792](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java#L782-L792));
   2. **узел, к которому подключение уже идёт** ([L793–L794](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java#L793-L794)):
      клиент ждёт его и не открывает новое;
   3. узел, к которому можно подключиться прямо сейчас. Из таких выбирается тот, к которому дольше всего не
      пытались подключаться ([L795–L800](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java#L795-L800));
   4. узлы, которые в паузе после неудачи (backoff, раздел 4), пропускаются
      ([L801–L804](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java#L801-L804),
      [ClusterConnectionStates.canConnect L77–L84](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/ClusterConnectionStates.java#L77-L84)).

3. Сам запрос метаданных тоже **не начинает новое подключение, пока идёт другое**:
   [NetworkClient L1348–L1355](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java#L1348-L1355). Комментарий в коде:
   *«If there's any connection establishment underway, wait until it completes. This prevents the client from
   unnecessarily connecting to additional nodes while a previous connection attempt has not been completed»*.
   Новое подключение открывается только если ни одно не идёт:
   [L1357–L1362](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java#L1357-L1362).

**Следствие:** если клиент случайно выбрал адрес, где пакеты молча теряются, он **просидит на нём весь таймаут
установки соединения** и только потом попробует следующий адрес. Это видно в пробе: при живом (отвечающем) `-1`
клиент 10 с ждал `-2` (раздел 7).

---

## 3. Сколько клиент ждёт на одном адресе

Попытка подключения начинается в `initiateConnect`
([NetworkClient L1136–L1153](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java#L1136-L1153)): узел переходит в состояние `CONNECTING`
([ClusterConnectionStates L147–L165](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/ClusterConnectionStates.java#L147-L165)), затем вызывается
неблокирующий `SocketChannel.connect` ([Selector L250–L272](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/common/network/Selector.java#L250-L272)).
Дальше три варианта.

### 3.1. Хост отвечает отказом (RST, порт закрыт)
- Ошибка приходит при завершении подключения (`finishConnect`) как `IOException`, канал закрывается:
  [Selector L528–L531](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/common/network/Selector.java#L528-L531),
  [L609–L612](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/common/network/Selector.java#L609-L612).
- Канал в этот момент в состоянии `NOT_CONNECTED`. Это состояние документировано как типичный признак
  неверного адреса bootstrap: [ChannelState L24–L27, L51](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/common/network/ChannelState.java#L24-L51).
- `NetworkClient` пишет WARN `Connection to node … could not be established. Node may not be available.`:
  [NetworkClient L898–L899](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java#L898-L899).
- Для bootstrap-узла дополнительно пишется WARN `Bootstrap broker … disconnected`:
  [NetworkClient L1251–L1256](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java#L1251-L1256).

**Время:** определяется сетью, по коду Kafka ожидания нет. В пробе — около 10 мс. В эксперименте 1 после
`docker kill kafka1` в 19:13:47 этот WARN появился в 19:13:47.555 (`logs/exp1/app.log`).

### 3.2. Пакеты молча теряются (нет ни ответа, ни отказа)
- Если узел дольше таймаута остаётся в `CONNECTING`, клиент сам закрывает сокет:
  [ClusterConnectionStates.isConnectionSetupTimeout L446–L451](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/ClusterConnectionStates.java#L446-L451),
  [NetworkClient.handleTimedOutConnections L939–L950](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java#L939-L950).
  В логе INFO `Disconnecting from node … due to socket connection setup timeout. The timeout value is … ms.`
- Таймаут задают `socket.connection.setup.timeout.ms` (по умолчанию **10 000 мс**) и
  `socket.connection.setup.timeout.max.ms` (по умолчанию **30 000 мс**):
  [CommonClientConfigs L139–L150](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/CommonClientConfigs.java#L139-L150),
  [ProducerConfig L487–L496](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java#L487-L496),
  [ConsumerConfig L599–L608](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerConfig.java#L599-L608).
  Документация:
  [socket.connection.setup.timeout.ms](https://kafka.apache.org/42/configuration/producer-configs/#producerconfigs_socket.connection.setup.timeout.ms),
  [socket.connection.setup.timeout.max.ms](https://kafka.apache.org/42/configuration/producer-configs/#producerconfigs_socket.connection.setup.timeout.max.ms).
- Если ОС сама вернёт ошибку раньше (например, «нет маршрута до хоста»), попытка завершится раньше таймаута
  Kafka, как в 3.1. Это поведение сети и ОС, а не Kafka.

**Как растёт таймаут** (раздел 4): первая попытка 8–12 с, после одной неудачи подряд 16–24 с, затем 24–30 с.

### 3.3. Имя брокера не резолвится в момент подключения
Это случай узлов, пришедших в метаданных от брокера: bootstrap-адреса с нерезолвящимися именами отброшены
ещё на шаге 1.
- `initiateConnect` получает `IOException` (в том числе `UnknownHostException`), пишет WARN
  `Error connecting to node …`, переводит узел в «отключён» и ставит паузу перед повтором:
  [NetworkClient L1146–L1151](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java#L1146-L1151).
- Мы видели это в эксперименте 2: admin-клиент в `kafka-tools` писал
  `Error connecting to node kafka1:29092 … UnknownHostException`, пока контейнер `kafka1` был остановлен.

---

## 4. Пауза перед повторной попыткой и рост таймаута

Обе величины считает `ExponentialBackoff` с основанием 2 и разбросом ±20 %:
[ClusterConnectionStates L40–L43, L51–L68](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/ClusterConnectionStates.java#L40-L68).

Формула:
```
значение(n) = min( initial × 2^min(n, log2(max/initial)) × случайное[0.8; 1.2) , max )
```
[ExponentialBackoff L43–L53, L59–L69](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/common/utils/ExponentialBackoff.java#L43-L69).

### 4.1. Пауза перед повтором к тому же узлу (`reconnect.backoff.ms` / `reconnect.backoff.max.ms`)
- По умолчанию 50 и 1000 мс:
  [ProducerConfig L416–L417](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java#L416-L417),
  [ConsumerConfig L515–L526](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerConfig.java#L515-L526).
  Документация: [reconnect.backoff.ms](https://kafka.apache.org/42/configuration/producer-configs/#producerconfigs_reconnect.backoff.ms),
  [reconnect.backoff.max.ms](https://kafka.apache.org/42/configuration/producer-configs/#producerconfigs_reconnect.backoff.max.ms).
- После каждой неудачи пауза растёт:
  [ClusterConnectionStates.disconnected L181–L198](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/ClusterConnectionStates.java#L181-L198),
  [updateReconnectBackoff L358–L368](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/ClusterConnectionStates.java#L358-L368).
- Пока пауза не истекла, к узлу не подключаются:
  [canConnect L77–L84](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/ClusterConnectionStates.java#L77-L84).
- После успешного подключения пауза сбрасывается:
  [ready L257–L264](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/ClusterConnectionStates.java#L257-L264).

| Неудача подряд | 1 | 2 | 3 | 4 | 5 | 6 и дальше |
|---|---|---|---|---|---|---|
| Пауза, мс | 40–60 | 80–120 | 160–240 | 320–480 | 640–960 | 800–1000 |

(Расчёт по формуле выше при 50/1000; верхняя граница обрезается до `max`.)

### 4.2. Таймаут установки соединения (`socket.connection.setup.timeout.ms` / `.max.ms`)
- Для нового узла берётся начальное значение со случайным разбросом:
  [ClusterConnectionStates L162–L163](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/ClusterConnectionStates.java#L162-L163).
- Если подключение не установилось, таймаут для этого узла растёт:
  [disconnected L184–L186](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/ClusterConnectionStates.java#L184-L186),
  [updateConnectionSetupTimeout L370–L380](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/ClusterConnectionStates.java#L370-L380).
- Сбрасывается, когда соединение установлено:
  [checkingApiVersions L246–L251](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/ClusterConnectionStates.java#L246-L251),
  [ready L257–L264](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/ClusterConnectionStates.java#L257-L264).

| Неудачных подключений подряд к этому узлу | 0 (первая попытка) | 1 | 2 и дальше |
|---|---|---|---|
| Таймаут, мс (10 000 / 30 000) | 8 000–12 000 | 16 000–24 000 | 24 000–30 000 |

Проба (раздел 7) показала 11 616 → 17 212 → 28 161 мс, а в другом прогоне 10 136 → 19 781 → 30 000 мс.
Это укладывается в таблицу.

---

## 5. Что видит приложение

### Продюсер
- `send()` ждёт метаданные топика не дольше `max.block.ms`:
  [KafkaProducer L995](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java#L995),
  [waitOnMetadata L1100–L1157](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java#L1100-L1157).
  По умолчанию **60 000 мс**: [ProducerConfig L435–L440](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java#L435-L440).
  Описание в коде: [L203–L209](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java#L203-L209), документация:
  [max.block.ms](https://kafka.apache.org/42/configuration/producer-configs/#producerconfigs_max.block.ms).
- Когда время истекло — `TimeoutException("Topic X not present in metadata after N ms.")`:
  [KafkaProducer L1129–L1148](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java#L1129-L1148),
  [L1159–L1165](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java#L1159-L1165).
  В пробе при `max.block.ms=45000` `send()` упал через **45 004 мс**.
- Значит, если до живого брокера клиент добирается дольше `max.block.ms` (несколько «молчащих» адресов, на каждом
  8–30 с), первые отправки падают по таймауту, хотя живые брокеры есть.
- **`close()` может висеть до конца текущей попытки подключения.** `close()` без аргументов ждёт поток отправки
  без ограничения по времени: [KafkaProducer L1368–L1371](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java#L1368-L1371),
  [L1421–L1423](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java#L1421-L1423). В пробе `close()` занял 12,7 с: поток
  отправки дожидался таймаута подключения к «молчащему» адресу. Это видно по стеку:
  `NetworkClientUtils.awaitReady → Selector.poll`.

### Консюмер
- Консюмер использует тот же механизм подключения (раздел 1). Аналога `max.block.ms` для метаданных у него нет.
  Блокирующие вызовы без явного таймаута ограничены `default.api.timeout.ms` (по умолчанию 60 000 мс:
  [ConsumerConfig L593–L598](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerConfig.java#L593-L598)).
- По умолчанию используется протокол групп `classic`:
  [ConsumerConfig L115](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerConfig.java#L115).

### Общее для всех запросов
- Если соединение есть, но ответ не пришёл, запрос обрывается по `request.timeout.ms` (по умолчанию 30 000 мс:
  [ProducerConfig L441–L446](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java#L441-L446),
  [ConsumerConfig L587–L592](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerConfig.java#L587-L592)). Клиент закрывает соединение с
  INFO `Disconnecting from node … due to request timeout.`:
  [NetworkClient L916–L924](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java#L916-L924).
  Это уже не про подключение, а про брокер, который принял соединение и не отвечает.

---

## 6. После первого ответа и rebootstrap

1. Когда пришёл ответ на запрос метаданных, временные bootstrap-узлы заменяются настоящими брокерами из ответа:
   [Metadata L356](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/Metadata.java#L356). Дальше клиент подключается к брокерам по адресам из
   метаданных (`advertised.listeners`), а не из `bootstrap.servers`
   ([NetworkClient L1197–L1199](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java#L1197-L1199)).
2. К `bootstrap.servers` клиент возвращается только при **rebootstrap**
   ([Metadata L313–L316](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/Metadata.java#L313-L316) пишет INFO `Rebootstrapping with …`).
   По умолчанию в 4.2.1 он включён: `metadata.recovery.strategy=rebootstrap`
   ([CommonClientConfigs L230–L242](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/CommonClientConfigs.java#L230-L242),
   [ProducerConfig L548–L560](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java#L548-L560),
   [ConsumerConfig L679–L691](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerConfig.java#L679-L691)).
   Срабатывает в двух случаях:
   - **все известные брокеры недоступны одновременно**: нет ни готового соединения, ни идущего подключения, ни узла,
     к которому можно подключиться прямо сейчас
     ([NetworkClient L1228–L1234](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java#L1228-L1234));
   - **метаданные не удаётся получить дольше `metadata.recovery.rebootstrap.trigger.ms`**, по умолчанию 300 000 мс
     ([CommonClientConfigs L244–L248](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/CommonClientConfigs.java#L244-L248),
     [NetworkClient L1117–L1118](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java#L1117-L1118),
     [L1313–L1316](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java#L1313-L1316)).
   Документация: [metadata.recovery.strategy](https://kafka.apache.org/42/configuration/producer-configs/#producerconfigs_metadata.recovery.strategy).
   Описание прямо советует при rebootstrap увеличивать `reconnect.backoff.*` и уменьшать
   `socket.connection.setup.timeout.*` ([CommonClientConfigs L238–L240](https://github.com/apache/kafka/blob/4.2.1/clients/src/main/java/org/apache/kafka/clients/CommonClientConfigs.java#L238-L240)).
3. **Для инцидента:** если брокер упал, а остальные живы, клиент к `bootstrap.servers` не возвращается, он работает
   с брокерами из метаданных. Поэтому мёртвые адреса в bootstrap-списке на отказ работающего клиента не влияют.
   Это подтвердил эксперимент 1. Они могут замедлить только **первое** подключение и rebootstrap, и только если
   по ним пакеты молча теряются (раздел 3.2).

---

## 7. Проверка на реальном клиенте (проба)

Настоящий `KafkaProducer` из kafka-clients 4.2.1 (jar взят из сборки нашего сервиса), живых брокеров нет вообще.
Bootstrap-список:
- `kafka-old-1.decommissioned.test:9092` — имя не резолвится;
- `localhost:59999` — порт закрыт, отказ;
- `10.255.255.1:9092` — пакеты молча теряются (проверено заранее: `nc -z -G 2 10.255.255.1 9092` не подключается).

`max.block.ms=45000`.

Код пробы (`Probe.java`, запуск `java -cp "lib/*" Probe.java "<bootstrap>" 45000`):
```java
Properties p = new Properties();
p.put("bootstrap.servers", args[0]);
p.put("max.block.ms", args[1]);
p.put("key.serializer", StringSerializer.class.getName());
p.put("value.serializer", StringSerializer.class.getName());
var producer = new KafkaProducer<String, String>(p);
long t1 = System.currentTimeMillis();
try { producer.send(new ProducerRecord<>("t", "v")).get(); }
catch (Exception e) { System.out.println("send failed after " + (System.currentTimeMillis() - t1) + " ms: " + e.getCause()); }
long t2 = System.currentTimeMillis();
producer.close();
System.out.println("close took " + (System.currentTimeMillis() - t2) + " ms");
```

Лог (DEBUG для `NetworkClient`, лишнее убрано):
```
20:36:10.070 WARN  ClientUtils - Couldn't resolve server kafka-old-1.decommissioned.test:9092 from bootstrap.servers as DNS resolution failed …
20:36:10.177 DEBUG NetworkClient - Initiating connection to node localhost:59999 (id: -1 …)
20:36:10.186 WARN  NetworkClient - Connection to node -1 (localhost/127.0.0.1:59999) could not be established. Node may not be available.
20:36:10.293 DEBUG NetworkClient - Initiating connection to node 10.255.255.1:9092 (id: -2 …)
20:36:21.922 INFO  NetworkClient - Disconnecting from node -2 due to socket connection setup timeout. The timeout value is 11616 ms.
20:36:22.025 DEBUG NetworkClient - Initiating connection to node localhost:59999 (id: -1 …)
20:36:22.027 WARN  NetworkClient - Connection to node -1 (localhost/127.0.0.1:59999) could not be established. …
20:36:22.131 DEBUG NetworkClient - Initiating connection to node 10.255.255.1:9092 (id: -2 …)
20:36:39.354 INFO  NetworkClient - Disconnecting from node -2 due to socket connection setup timeout. The timeout value is 17212 ms.
20:36:39.459 DEBUG NetworkClient - Initiating connection to node localhost:59999 (id: -1 …)
20:36:39.461 WARN  NetworkClient - Connection to node -1 (localhost/127.0.0.1:59999) could not be established. …
20:36:39.564 DEBUG NetworkClient - Initiating connection to node 10.255.255.1:9092 (id: -2 …)
send failed after 45004 ms: org.apache.kafka.common.errors.TimeoutException: Topic t not present in metadata after 45000 ms.
20:37:07.733 INFO  NetworkClient - Disconnecting from node -2 due to socket connection setup timeout. The timeout value is 28161 ms.
close took 12742 ms
```

| Наблюдение | Что подтверждает |
|---|---|
| нерезолвящийся адрес отброшен при создании, больше не встречается | раздел 1, п. 2 |
| узлы получили id -1 и -2 | раздел 1, п. 4 |
| отказ на `localhost:59999` — за 9 мс | раздел 3.1 |
| на `10.255.255.1` клиент ждал 11,6 с, и всё это время `-1` не пробовал | разделы 2 и 3.2 |
| таймаут для `-2` рос 11 616 → 17 212 → 28 161 мс | раздел 4.2 |
| `send()` упал ровно по `max.block.ms` (45 004 мс) | раздел 5 |
| `close()` ждал 12,7 с до конца текущей попытки подключения | раздел 5 |

В другом прогоне первым выбрался `-2`, а не `-1`: обход начинается со случайной позиции (раздел 2, п. 1).
Таймауты были 10 136 → 19 781 → 30 000 мс.

---

## 8. Что настраивать

| Настройка | По умолчанию | На что влияет |
|---|---|---|
| `socket.connection.setup.timeout.ms` | 10 000 | сколько ждать один «молчащий» адрес при первой попытке |
| `socket.connection.setup.timeout.max.ms` | 30 000 | потолок этого ожидания при повторных неудачах |
| `reconnect.backoff.ms` / `reconnect.backoff.max.ms` | 50 / 1000 | пауза перед повторным подключением к тому же адресу |
| `max.block.ms` (продюсер) | 60 000 | сколько `send()` ждёт метаданные, прежде чем упасть |
| `request.timeout.ms` | 30 000 | ожидание ответа на запрос по уже установленному соединению |
| `metadata.recovery.strategy` | `rebootstrap` | возвращаться ли к `bootstrap.servers`, когда все известные брокеры недоступны |
| `metadata.recovery.rebootstrap.trigger.ms` | 300 000 | через сколько без метаданных выполнять rebootstrap |
| `client.dns.lookup` | `use_all_dns_ips` | как резолвить имена; при нём нерезолвящийся bootstrap-адрес просто пропускается |

Самое надёжное — убрать мёртвые адреса из `bootstrap.servers`. Если адрес отвечает отказом или не резолвится,
он почти ничего не стоит. Если же по нему пакеты молча теряются, каждое попадание на него при первом подключении
и при rebootstrap стоит 8–30 секунд.
