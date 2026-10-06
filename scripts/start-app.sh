#!/usr/bin/env bash
# Builds (if needed) and starts the service in the background; logs go to logs/app.log.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
JAR=app/target/kafka-test-service-0.0.1-SNAPSHOT.jar
[[ -f "$JAR" ]] || (cd app && ./mvnw -q -DskipTests package)
mkdir -p logs
nohup java -jar "$JAR" >logs/app-stdout.log 2>&1 &
echo $! >logs/app.pid
echo "app pid $(cat logs/app.pid)"
for _ in $(seq 1 60); do
  curl -sf http://localhost:8080/actuator/health >/dev/null && { echo "app is UP"; exit 0; }
  sleep 1
done
echo "app did not become healthy" >&2; exit 1
