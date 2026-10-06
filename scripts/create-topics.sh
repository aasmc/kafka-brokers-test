#!/usr/bin/env bash
# Выполняется внутри контейнера kafka-tools при старте стенда (см. docker-compose.yml).
# Создаёт три тестовых топика: 1 партиция, RF=3, min.insync.replicas=2.
# Реплики назначаются явно, поэтому предпочтительный лидер известен заранее (первый в списке):
#   topic_producer   -> лидер kafka1 (реплики 1,2,3)
#   topic_consumer   -> лидер kafka1 (реплики 1,3,2)
#   topic_consumer_2 -> лидер kafka2 (реплики 2,3,1)
# kafka1 — третий хост в bootstrap-списке сервиса, то есть брокер, который «падает» в инциденте.

# Строгий режим: остановка при ошибке, при незаданной переменной и при ошибке внутри pipe.
set -euo pipefail

# Адреса всех трёх брокеров на внутреннем listener'е (доступны только внутри docker-сети).
BS="kafka1:29092,kafka2:29092,kafka3:29092"

# Функция create <топик> <назначение реплик>: создаёт топик; если он уже есть, ничего не делает.
create() {
  # аргументы функции: $1 — имя топика, $2 — назначение реплик (например, 1:2:3)
  local topic=$1 assignment=$2
  # --create --if-not-exists — создать, если топика ещё нет;
  # --replica-assignment 1:2:3 — одна партиция с репликами на брокерах 1, 2, 3 (первый — предпочтительный лидер);
  # --config min.insync.replicas=2 — запись с acks=all требует подтверждения минимум от 2 реплик.
  kafka-topics --bootstrap-server "$BS" --create --if-not-exists \
    --topic "$topic" --replica-assignment "$assignment" \
    --config min.insync.replicas=2
}

# Шаг 1. Создаём топики (вызов функции: имя функции и аргументы через пробел).
create topic_producer   1:2:3
create topic_consumer   1:3:2
create topic_consumer_2 2:3:1

# Шаг 2. Заставляем кластер создать __consumer_offsets прямо сейчас: топик создаётся при первом
#        запросе FindCoordinator. Нужно, чтобы до старта сервиса подобрать группы с нужными координаторами.
#   Временный консюмер в группе warmup читает topic_producer 5 секунд (--timeout-ms) и выходит;
#   вывод выбрасываем, а ошибку выхода по таймауту игнорируем (|| true).
kafka-console-consumer --bootstrap-server "$BS" --topic topic_producer --group warmup \
  --timeout-ms 5000 >/dev/null 2>&1 || true

# Шаг 3. Печатаем лидеров и ISR созданных топиков.
kafka-topics --bootstrap-server "$BS" --describe --topic topic_producer,topic_consumer,topic_consumer_2
# Сигнал для stand-up.sh, что топики готовы (он ищет эту строку в логе контейнера).
echo "TOPICS_READY"
