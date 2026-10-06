#!/usr/bin/env bash
# Тестовая нагрузка:
#   * по 1 сообщению в секунду в topic_consumer и в topic_consumer_2
#     (console producer внутри контейнера kafka-tools);
#   * 1 запрос в секунду на GET /api/v1/test (сервис отправляет сообщение в topic_producer).
# Работает до остановки (Ctrl+C / SIGTERM), но не дольше DURATION секунд (по умолчанию 600 = 10 минут).
# Логи: logs/$EXP/load/ — отправленные сообщения, вывод продюсеров, результаты HTTP-запросов.

# Остановка при незаданной переменной (-u) и ошибке в pipe (pipefail). Без -e: отдельная неудачная
# отправка или запрос не должны останавливать всю нагрузку.
set -uo pipefail

# Шаг 1. Параметры.
# Общая часть: ROOT, EXP, LOG_ROOT.
source "$(dirname "$0")/common.sh"
# Длительность, секунды (можно задать снаружи: DURATION=60 scripts/load.sh).
DURATION="${DURATION:-600}"
# Адрес тестового эндпоинта сервиса.
APP_URL="${APP_URL:-http://localhost:8080/api/v1/test}"
# Каталог логов нагрузки.
LOG_DIR="${LOG_DIR:-$LOG_ROOT/load}"
# Адреса брокеров для console producer (он работает внутри docker-сети).
BS="kafka1:29092,kafka2:29092,kafka3:29092"
# Момент автоостановки: date +%s — текущее время в секундах с 1970 года; $(( ... )) — арифметика.
DEADLINE=$(( $(date +%s) + DURATION ))

# Создаём каталог логов.
mkdir -p "$LOG_DIR"
# Функция now: текущее время в формате 2026-10-06T19:12:41.
now() { date '+%Y-%m-%dT%H:%M:%S'; }

# Функция generate <топик>: раз в секунду печатает строку "<топик> seq=N sent_at=<время>" до наступления DEADLINE.
# По номеру seq анализатор потом находит потерянные сообщения и дубли.
generate() {
  # $1 — имя топика; seq — счётчик сообщений
  local topic=$1 seq=0
  # while (( ... )) — пока текущее время меньше DEADLINE
  while (( $(date +%s) < DEADLINE )); do
    # увеличиваем счётчик на 1
    seq=$((seq + 1))
    # печатаем сообщение (оно уйдёт дальше по pipe)
    echo "$topic seq=$seq sent_at=$(now)"
    # пауза 1 секунда
    sleep 1
  done
}

# Функция produce <топик>: отправляет сообщения генератора в Kafka.
produce() {
  local topic=$1
  # generate         — поток строк, по одной в секунду;
  # tee -a файл      — дописывает каждую строку в sent-<топик>.log (что отправили) и передаёт её дальше;
  # docker exec -i   — запускает kafka-console-producer в контейнере kafka-tools; -i передаёт ему наш поток на вход;
  #                    каждая входная строка — одно сообщение;
  #   acks=all       — ждать подтверждения от всех реплик ISR; linger.ms=0 — отправлять сразу, не копить пачку;
  # >>файл 2>&1      — вывод и ошибки продюсера дописываем в producer-<топик>.log.
  generate "$topic" \
    | tee -a "$LOG_DIR/sent-$topic.log" \
    | docker exec -i kafka-tools kafka-console-producer \
        --bootstrap-server "$BS" --topic "$topic" \
        --producer-property acks=all --producer-property linger.ms=0 \
        >>"$LOG_DIR/producer-$topic.log" 2>&1
}

# Функция http_load: раз в секунду запрос на эндпоинт сервиса до наступления DEADLINE.
http_load() {
  # номер запроса
  local seq=0
  while (( $(date +%s) < DEADLINE )); do
    seq=$((seq + 1))
    # Каждый запрос выполняется в фоне (( ... ) &), чтобы зависший запрос не снижал темп ниже 1 запроса в секунду.
    (
      # время начала запроса
      ts=$(now)
      # curl: -s — без прогресса; -o /dev/null — тело ответа не нужно;
      #       -w — напечатать HTTP-код и длительность запроса в секундах; --max-time 20 — прервать запрос через 20 с.
      # || true — не считать ошибкой, если curl не смог соединиться.
      out=$(curl -s -o /dev/null -w '%{http_code} %{time_total}' --max-time 20 "$APP_URL" || true)
      # строка результата; ${out:-000 0} — «000 0», если curl ничего не вернул
      echo "$ts seq=$seq http=${out:-000 0}"
    ) >>"$LOG_DIR/http.log" &
    # пауза 1 секунда до следующего запроса
    sleep 1
  done
  # ждём завершения запросов, ещё выполняющихся в фоне
  wait
}

# Список PID фоновых процессов нагрузки.
PIDS=()
# Функция cleanup: останавливает всю нагрузку.
cleanup() {
  echo "$(now) нагрузка: останавливаем"
  # для каждого PID: pkill -P — завершить его дочерние процессы (generate, docker exec, curl), kill — сам процесс.
  # Когда генератор завершается, pipe закрывается, и console producer в контейнере тоже завершается.
  for p in "${PIDS[@]}"; do pkill -P "$p" 2>/dev/null; kill "$p" 2>/dev/null; done
  # ждём, пока все они действительно завершатся
  wait 2>/dev/null
  echo "$(now) нагрузка: остановлена"
}
# trap: при получении сигнала INT (Ctrl+C) или TERM (kill) вызвать cleanup и выйти с кодом 0.
trap 'cleanup; exit 0' INT TERM

# Шаг 2. Запускаем параллельно два продюсера и HTTP-нагрузку.
echo "$(now) нагрузка: старт, длительность=${DURATION}с, логи в $LOG_DIR"
# & — запустить функцию в фоне; PIDS+=($!) — запомнить PID фонового процесса.
produce topic_consumer   & PIDS+=($!)
produce topic_consumer_2 & PIDS+=($!)
http_load                & PIDS+=($!)

# Шаг 3. wait без аргументов — ждать завершения всех фоновых процессов
#        (они сами завершатся после DEADLINE, либо их остановит cleanup по сигналу).
wait
echo "$(now) нагрузка: завершена (истекло время)"
