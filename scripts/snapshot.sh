#!/usr/bin/env bash
# Снимок состояния кластера: лидеры и ISR топиков, состояние кворума KRaft, координаторы и лаг групп.
# Использование: snapshot.sh <метка>   (результат в logs/$EXP/snapshots/<метка>.txt и в stdout)

# Без -e: если одна из команд упадёт (например, без кворума), остальные части снимка всё равно соберутся.
set -uo pipefail
# Общая часть: ROOT, EXP, LOG_ROOT.
source "$(dirname "$0")/common.sh"

# Шаг 1. Файл для результата.
# Метка — первый аргумент скрипта; если его нет — "snapshot".
LABEL=${1:-snapshot}
# Путь к файлу снимка.
OUT="$LOG_ROOT/snapshots/$LABEL.txt"
# Создаём каталог snapshots (dirname — каталог файла).
mkdir -p "$(dirname "$OUT")"
# Адреса брокеров (команды выполняются внутри docker-сети).
BS="kafka1:29092,kafka2:29092,kafka3:29092"

# Шаг 2. Id групп, которые реально использует сервис.
# Значения по умолчанию (как в application.yml)...
G1=kafka-test-service-consumer-1; G2=kafka-test-service-consumer-2
# ...но если stand-up.sh подобрал другие и сохранил их в groups.env — берём их.
if [[ -f "$LOG_ROOT/groups.env" ]]; then
  # source groups.env задаёт APP_GROUPS_CONSUMER1/2; копируем их в G1/G2.
  # shellcheck disable=SC1091
  source "$LOG_ROOT/groups.env"; G1=$APP_GROUPS_CONSUMER1; G2=$APP_GROUPS_CONSUMER2
fi

# Функция run <команда...>: выполняет CLI-команду Kafka в контейнере kafka-tools и печатает результат.
#   echo "### $*"  — заголовок с текстом команды ($* — все аргументы одной строкой);
#   timeout 20     — прервать команду через 20 секунд: без кворума часть админ-запросов зависает;
#   grep -v -E     — убрать шумные строки (WARN о недоступном брокере, стектрейсы); последний echo — пустая строка-разделитель.
run() { echo "### $*"; docker exec kafka-tools timeout 20 "$@" 2>&1 | grep -v -E "WARN|Connection to node|UnknownHostException|^\s+at " ; echo; }

# Шаг 3. Собираем снимок. Вывод всего блока { ... } передаём в tee: он пишет его и в файл, и на экран.
{
  # заголовок: метка и время
  echo "# $LABEL @ $(date '+%Y-%m-%dT%H:%M:%S')"
  # какие брокеры сейчас запущены: docker ps с фильтром по имени, печатаем имя и статус
  echo "### docker ps"; docker ps --filter name=kafka --format '{{.Names}}\t{{.Status}}'; echo
  # лидер и ISR каждого тестового топика
  run kafka-topics --bootstrap-server "$BS" --describe --topic topic_producer,topic_consumer,topic_consumer_2
  # лидер кворума контроллеров и список голосующих участников
  run kafka-metadata-quorum --bootstrap-server "$BS" describe --status
  # координатор и состояние (Stable, Empty, ...) каждой группы
  run kafka-consumer-groups --bootstrap-server "$BS" --describe --state --group "$G1"
  run kafka-consumer-groups --bootstrap-server "$BS" --describe --state --group "$G2"
  # закоммиченные offset'ы, конец лога и лаг каждой группы
  run kafka-consumer-groups --bootstrap-server "$BS" --describe --group "$G1"
  run kafka-consumer-groups --bootstrap-server "$BS" --describe --group "$G2"
} | tee "$OUT"
