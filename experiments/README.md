# Профили экспериментов

Каждый файл `<имя>.env` описывает один эксперимент:

| Переменная | Значение |
|---|---|
| `EXP_TITLE` | короткое описание эксперимента |
| `COMPOSE_FILES` | compose-файлы стенда через пробел, применяются по порядку |
| `SPRING_PROFILE` | Spring-профиль сервиса (`app/src/main/resources/application-<профиль>.yml`) |
| `TARGET`, `MODE` | какой контейнер ронять и как (`kill`, `stop`, `pause`) |
| `BASELINE`, `DOWN`, `RECOVERY` | длительность фаз, секунды |
| `GROUP1_COORDINATOR`, `GROUP2_COORDINATOR` | на каких брокерах должны быть координаторы групп консюмеров |

Запуск: `EXP=exp2 scripts/stand-up.sh && EXP=exp2 scripts/experiment.sh`
