# Общая часть для всех скриптов: подключается через `source`, сам по себе не запускается.
# Загружает профиль эксперимента experiments/$EXP.env и готовит общие переменные и функции.
#
# Памятка по синтаксису bash, который встречается в скриптах:
#   VAR=значение          — присвоить переменную (без пробелов вокруг «=»); "$VAR" — подставить её значение
#   "${VAR:-def}"         — значение VAR, а если она пустая или не задана — def
#   $(команда)            — подставить вывод команды
#   [[ условие ]]         — проверка условия: -f файл существует, -x файл исполняемый, -n строка не пустая
#   a && b                — выполнить b, только если a успешна; a || b — выполнить b, только если a неуспешна
#   >файл, >>файл         — записать вывод в файл (перезаписать / дописать); 2>&1 — ошибки туда же, куда вывод
#   >&2                   — вывести в поток ошибок (stderr); /dev/null — «чёрная дыра», вывод выбрасывается
#   a | b                 — передать вывод команды a на вход команде b (pipe)
#   команда &             — запустить в фоне, не дожидаясь завершения; $! — PID последнего фонового процесса
#   $1, $2                — аргументы скрипта или функции; local — переменная видна только внутри функции

# Шаг 1. Корень репозитория: каталог на уровень выше scripts/.
#   ${BASH_SOURCE[0]} — путь к этому файлу, dirname — его каталог (scripts), /.. — на уровень выше,
#   cd ... && pwd — перейти туда и напечатать абсолютный путь. Результат сохраняем в ROOT.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Шаг 2. Имя профиля эксперимента: берём из переменной окружения EXP, а если её нет — exp1.
EXP="${EXP:-exp1}"
# Если файла experiments/<профиль>.env нет (-f — «файл существует»), печатаем ошибку в stderr и выходим с кодом 1.
[[ -f "$ROOT/experiments/$EXP.env" ]] || { echo "неизвестный профиль эксперимента: $EXP" >&2; exit 1; }

# Шаг 3. Загружаем переменные профиля: COMPOSE_FILES, SPRING_PROFILE, TARGET, MODE, тайминги и т.д.
#   source выполняет файл в текущем shell, поэтому заданные в нём переменные становятся доступны здесь.
# shellcheck disable=SC1090
source "$ROOT/experiments/$EXP.env"

# Шаг 4. У каждого эксперимента свой каталог логов: logs/<профиль>/.
LOG_ROOT="$ROOT/logs/$EXP"

# Шаг 5. Собираем аргументы `-f <файл>` для docker compose из списка COMPOSE_FILES.
COMPOSE_ARGS=()                                                    # пустой массив
for f in $COMPOSE_FILES; do COMPOSE_ARGS+=(-f "$ROOT/$f"); done    # для каждого файла из списка добавляем в массив два элемента: -f и путь

# Шаг 6. Функция-обёртка: compose <команда> = docker compose с файлами текущего профиля.
#   --project-directory — относительные пути в compose-файлах считаются от корня репозитория;
#   "${COMPOSE_ARGS[@]}" — все элементы массива (-f файл1 -f файл2 ...); "$@" — все аргументы, переданные функции.
compose() { docker compose --project-directory "$ROOT" "${COMPOSE_ARGS[@]}" "$@"; }

# Шаг 7. export делает переменную видимой для дочерних процессов: load.sh и snapshot.sh,
#        запущенные из других скриптов, работают с тем же профилем.
export EXP

# Минимальная версия Java для сборки и запуска сервиса (java.version в app/pom.xml).
REQUIRED_JAVA=25

# Функция: печатает мажорную версию java по пути $1 (например, 25) или ничего, если java не запускается.
java_major() {
  # java -XshowSettings:properties -version печатает свойства JVM (в stderr, поэтому 2>&1 перенаправляет его в pipe);
  # awk находит строку java.specification.version = 25 и печатает число до точки
  # (для старых версий вида 1.8 печатает вторую часть — 8).
  "$1" -XshowSettings:properties -version 2>&1 \
    | awk -F'= ' '/java.specification.version/ {split($2, v, "."); print (v[1] == "1" ? v[2] : v[1])}'
}

# Функция: находит JDK версии REQUIRED_JAVA или новее и экспортирует JAVA_HOME и PATH для mvnw и java.
# Вызывается только скриптами, которым нужна Java (start-app.sh). Порядок поиска:
#   1) JAVA_HOME, если задан;
#   2) на macOS — /usr/libexec/java_home -v 25+ (самый новый установленный JDK 25 и выше);
#   3) java из PATH.
# Чтобы выбрать конкретный JDK, задайте JAVA_HOME перед запуском: JAVA_HOME=/path/to/jdk-25 scripts/...
require_java() {
  # локальные переменные функции: список кандидатов (массив), каталог JDK, путь к java, версия
  local candidates=() home java major

  # Кандидат 1: если JAVA_HOME не пустая (-n), добавляем её в список.
  [[ -n "${JAVA_HOME:-}" ]] && candidates+=("$JAVA_HOME")

  # Кандидат 2: утилита java_home есть только на macOS (-x — «файл исполняемый»).
  if [[ -x /usr/libexec/java_home ]]; then
    # спрашиваем у неё путь к JDK 25+; ошибки выбрасываем (2>/dev/null), а при неудаче (|| true) получаем пустую строку
    home=$(/usr/libexec/java_home -v "${REQUIRED_JAVA}+" 2>/dev/null || true)
    # если путь получен, добавляем его в список
    [[ -n "$home" ]] && candidates+=("$home")
  fi

  # Кандидат 3: java из PATH. command -v печатает путь к команде или завершается с ошибкой, если её нет.
  if command -v java >/dev/null 2>&1; then
    # путь к java, например /usr/bin/java
    java=$(command -v java)
    # readlink -f раскрывает симлинки до настоящего файла (…/jdk-25/bin/java); если readlink -f недоступен, берём путь как есть;
    # dirname даёт каталог bin, /.. — каталог JDK; cd ... && pwd превращает его в абсолютный путь
    home=$(cd "$(dirname "$(readlink -f "$java" 2>/dev/null || echo "$java")")/.." && pwd)
    candidates+=("$home")
  fi

  # Проверяем кандидатов по очереди, берём первого с версией не ниже REQUIRED_JAVA.
  for home in "${candidates[@]}"; do
    java="$home/bin/java"
    # если в каталоге нет исполняемого bin/java — переходим к следующему кандидату (continue)
    [[ -x "$java" ]] || continue
    # узнаём версию этой java
    major=$(java_major "$java")
    # (( ... )) — арифметическое сравнение чисел
    if [[ -n "$major" ]] && (( major >= REQUIRED_JAVA )); then
      # подходит: JAVA_HOME нужен mvnw, а PATH — чтобы команда java запускала именно этот JDK
      export JAVA_HOME="$home" PATH="$home/bin:$PATH"
      echo "Java $major: $JAVA_HOME"
      # выходим из функции с кодом 0 (успех)
      return 0
    fi
    # не подходит по версии: сообщаем в stderr; ${major:-?} — «?», если версию узнать не удалось
    echo "пропускаем $home: Java ${major:-?}, нужна $REQUIRED_JAVA+" >&2
  done

  # Никто не подошёл: подсказка и код 1 (ошибка). В скриптах с set -e это остановит выполнение.
  echo "не найден JDK $REQUIRED_JAVA+. Установите его или укажите путь: JAVA_HOME=/path/to/jdk-$REQUIRED_JAVA" >&2
  return 1
}
