# Стенд: отказ брокера Kafka при «мёртвых» хостах в bootstrap-servers

- `docker-compose.yml` — 3 брокера `confluentinc/cp-kafka:7.9.0` (Kafka 3.9.0) в KRaft-кластере и контейнер `kafka-tools`, который создаёт топики
- `app/` — Spring Boot 4.1.1 / Java 25: 1 продюсер (`topic_producer`, `GET /api/v1/test`), 2 консюмера (`topic_consumer`, `topic_consumer_2`), в bootstrap 5 хостов, из них 2 несуществующих
- `scripts/` — запуск, нагрузка, эксперимент, анализ
- `EXPERIMENT-RESULTS.md` — результаты; `kafka-incident-analysis.md` — разбор инцидента

## Запуск
```bash
docker compose up -d                 # кластер + создание топиков
scripts/start-app.sh                 # сборка и запуск сервиса (Java 25), лог в logs/app.log
curl localhost:8080/api/v1/test      # проверка

DURATION=600 scripts/load.sh         # только нагрузка: до Ctrl+C, но не дольше 10 минут
MODE=kill scripts/experiment.sh      # нагрузка + отказ kafka1 + снапшоты + сводка (MODE=kill|stop|pause)
python3 scripts/analyze.py           # повторно построить сводку по логам
scripts/snapshot.sh my-label         # лидеры, ISR, кворум и координаторы групп

scripts/stop-app.sh && docker compose down -v
```
