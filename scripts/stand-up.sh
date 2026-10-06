#!/usr/bin/env bash
# Пересоздаёт стенд выбранного профиля с нуля и запускает сервис.
# Использование: EXP=exp2 scripts/stand-up.sh

# Строгий режим: остановка при ошибке (-e), при незаданной переменной (-u) и при ошибке внутри pipe (pipefail).
set -euo pipefail
# Подключаем общую часть: ROOT, EXP, LOG_ROOT, переменные профиля, функцию compose.
source "$(dirname "$0")/common.sh"

# Печатаем, какой стенд поднимаем (EXP_TITLE задан в профиле).
echo "== стенд $EXP: $EXP_TITLE"

# Шаг 1. Останавливаем сервис, если он был запущен. || true — не считать ошибкой, если сервис и так не работал.
"$ROOT/scripts/stop-app.sh" || true

# Шаг 2. Удаляем контейнеры и тома предыдущего стенда (любого профиля), чтобы стенд стартовал с чистыми данными.
#   down — остановить и удалить контейнеры и сеть; -v — удалить тома с данными Kafka;
#   --remove-orphans — удалить и контейнеры, которых нет в compose-файле. Вывод и ошибки скрываем.
docker compose --project-directory "$ROOT" -f "$ROOT/docker-compose.yml" down -v --remove-orphans >/dev/null 2>&1 || true

# Шаг 3. Поднимаем брокеры и kafka-tools с compose-файлами профиля (-d — в фоне, не занимая терминал).
compose up -d

# Шаг 4. Ждём готовности топиков: create-topics.sh в контейнере kafka-tools в конце печатает TOPICS_READY.
#   until ...; do ...; done — повторять тело цикла, пока условие не станет истинным;
#   docker logs kafka-tools — вывод контейнера; grep -q — тихий поиск строки (только код успеха/неуспеха).
until docker logs kafka-tools 2>&1 | grep -q TOPICS_READY; do sleep 2; done
# Печатаем из лога kafka-tools только строки с лидером и ISR партиций (grep -E — поиск по регулярному выражению).
docker logs kafka-tools 2>&1 | grep -E "^\s*Topic:.*Partition"

# Шаг 5. Для exp2 показываем состав кворума: в нём должно быть 5 голосов, из них 2 «выведенных».
if [[ "$EXP" == exp2 ]]; then
  echo "== кворум"
  # docker exec <контейнер> <команда> — выполнить команду внутри работающего контейнера;
  # kafka-metadata-quorum describe --status — состояние кворума; оставляем строки с лидером и списком голосов.
  docker exec kafka-tools kafka-metadata-quorum --bootstrap-server kafka1:29092 describe --status | grep -E "LeaderId|CurrentVoters"
fi

# Шаг 6. Сохраняем размещение партиций __consumer_offsets: лидер партиции — это координатор группы.
mkdir -p "$LOG_ROOT"
# kafka-topics --describe печатает лидера и реплики каждой партиции; «\» переносит команду на следующую строку.
docker exec kafka-tools kafka-topics --bootstrap-server kafka1:29092 --describe --topic __consumer_offsets \
  >"$LOG_ROOT/consumer-offsets-describe.txt"

# Шаг 7. Подбираем id групп консюмеров так, чтобы их координаторы оказались на брокерах
#        GROUP1_COORDINATOR и GROUP2_COORDINATOR из профиля.
#   pick-groups.py печатает строку вида "APP_GROUPS_CONSUMER1=... APP_GROUPS_CONSUMER2=...", сохраняем её в GROUPS_ENV.
GROUPS_ENV=$(python3 "$ROOT/scripts/pick-groups.py" "$LOG_ROOT/consumer-offsets-describe.txt" \
  "$GROUP1_COORDINATOR" "$GROUP2_COORDINATOR")
# Печатаем выбранные id групп.
echo "== группы: $GROUPS_ENV"
# Сохраняем их в groups.env: snapshot.sh читает оттуда реальные id групп.
echo "$GROUPS_ENV" >"$LOG_ROOT/groups.env"

# Шаг 8. Запускаем сервис. env A=1 B=2 команда — запустить команду с этими переменными окружения;
#        $GROUPS_ENV без кавычек нарочно: строка разбивается по пробелам на отдельные A=... и B=...
env $GROUPS_ENV "$ROOT/scripts/start-app.sh"
