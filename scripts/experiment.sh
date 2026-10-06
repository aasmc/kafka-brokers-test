#!/usr/bin/env bash
# Broker failure experiment.
#   MODE=kill  (default) - docker kill: abrupt crash, no controlled shutdown
#   MODE=stop            - docker stop: graceful shutdown (controlled leader migration)
#   MODE=pause           - docker pause: process frozen, TCP hangs (closest to "host unreachable")
# Timeline: load starts -> BASELINE s -> broker down -> DOWN s -> broker back -> RECOVERY s -> load stops.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

TARGET="${TARGET:-kafka1}"   # kafka1 = 3rd host in the bootstrap list, leader of topic_producer and topic_consumer
MODE="${MODE:-kill}"
BASELINE="${BASELINE:-60}"
DOWN="${DOWN:-180}"
RECOVERY="${RECOVERY:-90}"
EXP_DIR="$ROOT/logs/experiment"
mkdir -p "$EXP_DIR"
EVENTS="$EXP_DIR/events.log"
: >"$EVENTS"

event() { echo "$(date '+%Y-%m-%dT%H:%M:%S') $*" | tee -a "$EVENTS"; }

event "EXPERIMENT target=$TARGET mode=$MODE baseline=${BASELINE}s down=${DOWN}s recovery=${RECOVERY}s"
scripts/snapshot.sh 01-before >/dev/null

DURATION=600 scripts/load.sh >"$EXP_DIR/load-console.log" 2>&1 &
LOAD_PID=$!
event "LOAD_STARTED pid=$LOAD_PID"
sleep "$BASELINE"

case "$MODE" in
  kill)  docker kill "$TARGET" >/dev/null ;;
  stop)  docker stop "$TARGET" >/dev/null ;;
  pause) docker pause "$TARGET" >/dev/null ;;
esac
event "BROKER_DOWN $TARGET ($MODE)"

sleep 15;  scripts/snapshot.sh 02-down-15s  >/dev/null; event "SNAPSHOT 02-down-15s"
sleep 45;  scripts/snapshot.sh 03-down-60s  >/dev/null; event "SNAPSHOT 03-down-60s"
sleep $(( DOWN - 60 > 0 ? DOWN - 60 : 0 ))
scripts/snapshot.sh 04-down-end >/dev/null; event "SNAPSHOT 04-down-end"

case "$MODE" in
  pause) docker unpause "$TARGET" >/dev/null ;;
  *)     docker start "$TARGET" >/dev/null ;;
esac
event "BROKER_UP $TARGET"
sleep "$RECOVERY"
scripts/snapshot.sh 05-after-recovery >/dev/null; event "SNAPSHOT 05-after-recovery"

kill -TERM "$LOAD_PID"; wait "$LOAD_PID" 2>/dev/null
event "LOAD_STOPPED"
sleep 5   # let consumers drain

mkdir -p "$ROOT/logs/brokers"
for b in kafka1 kafka2 kafka3; do docker logs -t "$b" >"$ROOT/logs/brokers/$b.log" 2>&1; done
event "BROKER_LOGS_COLLECTED"

python3 scripts/analyze.py | tee "$EXP_DIR/summary.txt"
