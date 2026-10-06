#!/usr/bin/env bash
# Собирает сервис и запускает его в фоне со Spring-профилем текущего эксперимента.
# Лог сервиса: logs/$EXP/app.log. Id групп можно переопределить через APP_GROUPS_CONSUMER1/2.

# Строгий режим: -e — остановиться при первой ошибке, -u — ошибка при обращении к незаданной переменной,
# -o pipefail — ошибка в любой команде pipe считается ошибкой всего pipe.
set -euo pipefail
# Подключаем общую часть: ROOT, EXP, LOG_ROOT, SPRING_PROFILE, функцию require_java.
source "$(dirname "$0")/common.sh"
# Переходим в корень репозитория, чтобы относительные пути (app/...) работали.
cd "$ROOT"

# Шаг 1. Находим JDK 25+ (функция из common.sh). Если не найден — из-за set -e скрипт остановится здесь.
require_java

# Шаг 2. Собираем jar через Maven Wrapper (Maven скачается сам), тесты пропускаем.
# Путь к собранному jar.
JAR=app/target/kafka-test-service-0.0.1-SNAPSHOT.jar
# Создаём каталог логов (-p — вместе с родительскими, без ошибки, если уже есть).
mkdir -p "$LOG_ROOT"
# ( cd app && ./mvnw ... ) — сборка в подоболочке: cd действует только внутри скобок.
#   -q — тихий режим, -DskipTests — без тестов, package — собрать jar. Весь вывод (> и 2>&1) — в build.log.
# if ! ...; then — если сборка завершилась ошибкой:
if ! (cd app && ./mvnw -q -DskipTests package) >"$LOG_ROOT/build.log" 2>&1; then
  # показываем лог сборки в stderr,
  cat "$LOG_ROOT/build.log" >&2
  # пишем, где его найти,
  echo "сборка сервиса не удалась, лог: $LOG_ROOT/build.log" >&2
  # и выходим с ошибкой.
  exit 1
fi

# Шаг 3. Запускаем сервис в фоне.
#   APP_LOG_FILE=... перед командой — переменная окружения только для этой команды
#                    (путь к логу, см. logging.file.name в application.yml);
#   nohup            — процесс не завершится, когда закроется терминал;
#   java -jar        — запуск jar; --spring.profiles.active — Spring-профиль из профиля эксперимента;
#   >... 2>&1        — stdout и stderr в app-stdout.log; & в конце — в фоне. «\» переносит команду на следующую строку.
APP_LOG_FILE="$LOG_ROOT/app.log" nohup java -jar "$JAR" --spring.profiles.active="$SPRING_PROFILE" \
  >"$LOG_ROOT/app-stdout.log" 2>&1 &

# Шаг 4. Запоминаем PID: по нему stop-app.sh остановит сервис. $! — PID только что запущенного фонового процесса.
echo $! >"$ROOT/logs/app.pid"
# Сообщаем PID, профиль и путь к логу.
echo "pid сервиса $(cat "$ROOT/logs/app.pid"), профиль $SPRING_PROFILE, лог $LOG_ROOT/app.log"

# Шаг 5. Ждём до 60 секунд, пока сервис не ответит на health-check.
#   seq 1 60 — числа от 1 до 60, цикл выполнится 60 раз (_ — переменная, значение которой не используется).
for _ in $(seq 1 60); do
  # curl -s — без прогресса, -f — неуспех при HTTP-ошибке; если health ответил 200 — пишем «UP» и выходим с успехом.
  curl -sf http://localhost:8080/actuator/health >/dev/null && { echo "сервис поднят (UP)"; exit 0; }
  # пауза 1 секунда перед следующей попыткой
  sleep 1
done
# За 60 секунд сервис не поднялся — ошибка.
echo "сервис не стал healthy за 60 секунд" >&2; exit 1
