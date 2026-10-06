#!/usr/bin/env bash
# Эксперимент с отказом брокера под нагрузкой.
# Параметры берутся из профиля experiments/$EXP.env (по умолчанию EXP=exp1).
# Способ отказа (MODE):
#   kill  (по умолчанию) — docker kill: аварийное падение, без корректного завершения брокера;
#   stop                 — docker stop: корректное завершение (лидерство передаётся штатно);
#   pause                — docker pause: процесс заморожен, TCP-соединения зависают (ближе всего к «хост недоступен»).
# Ход: старт нагрузки -> BASELINE с -> отказ брокера -> DOWN с -> брокер возвращается -> RECOVERY с -> стоп нагрузки.

# Без -e: эксперимент должен дойти до конца (сбор логов, сводка), даже если какая-то команда завершилась ошибкой.
set -uo pipefail
# Общая часть: ROOT, EXP, LOG_ROOT, переменные профиля.
source "$(dirname "$0")/common.sh"
# Работаем из корня репозитория: ниже вызываются scripts/... по относительным путям.
cd "$ROOT"

# Шаг 1. Параметры эксперимента: значения из профиля, а если их там нет — значения после «:-».
TARGET="${TARGET:-kafka1}"     # какой контейнер ронять
MODE="${MODE:-kill}"           # как ронять
BASELINE="${BASELINE:-60}"     # сколько секунд нагрузки до отказа
DOWN="${DOWN:-180}"            # сколько секунд брокер лежит
RECOVERY="${RECOVERY:-90}"     # сколько секунд наблюдаем после возврата брокера

# Шаг 2. Журнал событий эксперимента: по его отметкам анализатор делит время на фазы.
# Каталог для журнала и сводки.
EXP_DIR="$LOG_ROOT/experiment"
mkdir -p "$EXP_DIR"
EVENTS="$EXP_DIR/events.log"
# : >файл — очистить файл (создать пустой): «:» — команда, которая ничего не делает.
: >"$EVENTS"
# Функция event <текст>: печатает событие с временем на экран и дописывает его в журнал (tee -a).
event() { echo "$(date '+%Y-%m-%dT%H:%M:%S') $*" | tee -a "$EVENTS"; }

# Шаг 3. Записываем параметры эксперимента и делаем снимок кластера до отказа (вывод снимка на экран не нужен).
event "EXPERIMENT exp=$EXP ($EXP_TITLE) target=$TARGET mode=$MODE baseline=${BASELINE}s down=${DOWN}s recovery=${RECOVERY}s"
scripts/snapshot.sh 01-before >/dev/null

# Шаг 4. Запускаем нагрузку в фоне (с запасом на 10 минут; остановим её сами в шаге 8).
#        Её консольный вывод — в load-console.log; $! — PID процесса нагрузки.
DURATION=600 scripts/load.sh >"$EXP_DIR/load-console.log" 2>&1 &
LOAD_PID=$!
event "LOAD_STARTED pid=$LOAD_PID"
# Даём нагрузке поработать BASELINE секунд: это эталон нормальной работы.
sleep "$BASELINE"

# Шаг 5. Роняем брокер выбранным способом. case — выбор ветки по значению MODE.
case "$MODE" in
  kill)  docker kill "$TARGET" >/dev/null ;;    # мгновенно убить процесс (SIGKILL)
  stop)  docker stop "$TARGET" >/dev/null ;;    # корректно остановить (SIGTERM, затем ожидание)
  pause) docker pause "$TARGET" >/dev/null ;;   # заморозить все процессы контейнера
esac
event "BROKER_DOWN $TARGET ($MODE)"

# Шаг 6. Снимки во время отказа: через 15 с, через 60 с и в конце фазы DOWN.
#        ( команда & ) — запустить снимок в фоне: без кворума часть админ-команд висит до таймаута
#        и сдвигала бы хронологию эксперимента.
sleep 15;  (scripts/snapshot.sh 02-down-15s >/dev/null &); event "SNAPSHOT 02-down-15s"
sleep 45;  (scripts/snapshot.sh 03-down-60s >/dev/null &); event "SNAPSHOT 03-down-60s"
# Досыпаем до конца фазы DOWN: DOWN - 60 секунд (уже прошло 15 + 45), но не меньше нуля.
#   $(( условие ? a : b )) — арифметическое «если условие, то a, иначе b».
sleep $(( DOWN - 60 > 0 ? DOWN - 60 : 0 ))
(scripts/snapshot.sh 04-down-end >/dev/null &); event "SNAPSHOT 04-down-end"

# Шаг 7. Возвращаем брокер: после pause — unpause, в остальных случаях — запустить контейнер заново (* — «всё остальное»).
case "$MODE" in
  pause) docker unpause "$TARGET" >/dev/null ;;
  *)     docker start "$TARGET" >/dev/null ;;
esac
event "BROKER_UP $TARGET"
# Наблюдаем восстановление RECOVERY секунд, затем снимок после восстановления.
sleep "$RECOVERY"
(scripts/snapshot.sh 05-after-recovery >/dev/null &); event "SNAPSHOT 05-after-recovery"

# Шаг 8. Останавливаем нагрузку: kill -TERM отправляет сигнал TERM, load.sh по нему вызывает свой cleanup;
#        wait ждёт, пока процесс нагрузки завершится.
kill -TERM "$LOAD_PID"; wait "$LOAD_PID" 2>/dev/null
event "LOAD_STOPPED"
# Даём консюмерам 5 секунд дочитать остаток.
sleep 5

# Шаг 9. Ждём, пока завершатся фоновые снимки: pgrep -f ищет процессы по командной строке;
#        пока такие есть — проверяем раз в секунду.
while pgrep -f "scripts/snapshot.sh" >/dev/null; do sleep 1; done

# Шаг 10. Сохраняем логи брокеров: docker logs -t — весь вывод контейнера с метками времени docker.
mkdir -p "$LOG_ROOT/brokers"
for b in kafka1 kafka2 kafka3; do docker logs -t "$b" >"$LOG_ROOT/brokers/$b.log" 2>&1; done
event "BROKER_LOGS_COLLECTED"

# Шаг 11. Строим сводку по логам сервиса, нагрузки и событиям; tee показывает её и сохраняет в summary.txt.
python3 scripts/analyze.py "$LOG_ROOT" | tee "$EXP_DIR/summary.txt"
