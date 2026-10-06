#!/usr/bin/env bash
# Test load:
#   * 1 msg/s into topic_consumer and 1 msg/s into topic_consumer_2 (console producers in the kafka-tools container)
#   * 1 req/s to GET /api/v1/test (the service sends into topic_producer)
# Runs until stopped (Ctrl+C / SIGTERM) but no longer than DURATION seconds (default 600 = 10 min).
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DURATION="${DURATION:-600}"
APP_URL="${APP_URL:-http://localhost:8080/api/v1/test}"
LOG_DIR="${LOG_DIR:-$ROOT/logs/load}"
BS="kafka1:29092,kafka2:29092,kafka3:29092"
DEADLINE=$(( $(date +%s) + DURATION ))

mkdir -p "$LOG_DIR"
now() { date '+%Y-%m-%dT%H:%M:%S'; }

# Prints one "seq=N sent_at=..." line per second until the deadline.
generate() {
  local topic=$1 seq=0
  while (( $(date +%s) < DEADLINE )); do
    seq=$((seq + 1))
    echo "$topic seq=$seq sent_at=$(now)"
    sleep 1
  done
}

produce() {
  local topic=$1
  generate "$topic" \
    | tee -a "$LOG_DIR/sent-$topic.log" \
    | docker exec -i kafka-tools kafka-console-producer \
        --bootstrap-server "$BS" --topic "$topic" \
        --producer-property acks=all --producer-property linger.ms=0 \
        >>"$LOG_DIR/producer-$topic.log" 2>&1
}

http_load() {
  local seq=0
  while (( $(date +%s) < DEADLINE )); do
    seq=$((seq + 1))
    # each request in background, so a hanging request does not lower the rate below 1 req/s
    (
      ts=$(now)
      out=$(curl -s -o /dev/null -w '%{http_code} %{time_total}' --max-time 20 "$APP_URL" || true)
      echo "$ts seq=$seq http=${out:-000 0}"
    ) >>"$LOG_DIR/http.log" &
    sleep 1
  done
  wait
}

PIDS=()
cleanup() {
  echo "$(now) load: stopping"
  for p in "${PIDS[@]}"; do pkill -P "$p" 2>/dev/null; kill "$p" 2>/dev/null; done
  wait 2>/dev/null
  echo "$(now) load: stopped"
}
trap 'cleanup; exit 0' INT TERM

echo "$(now) load: started, duration=${DURATION}s, logs in $LOG_DIR"
produce topic_consumer   & PIDS+=($!)
produce topic_consumer_2 & PIDS+=($!)
http_load                & PIDS+=($!)

wait
echo "$(now) load: finished (deadline reached)"
