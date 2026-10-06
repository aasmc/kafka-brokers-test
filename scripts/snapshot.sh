#!/usr/bin/env bash
# Cluster state snapshot: topic leaders/ISR, group coordinators, KRaft quorum.
# Usage: snapshot.sh <label>   (output goes to logs/snapshots/<label>.txt and stdout)
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LABEL=${1:-snapshot}
OUT="$ROOT/logs/snapshots/$LABEL.txt"
mkdir -p "$(dirname "$OUT")"
BS="kafka1:29092,kafka2:29092,kafka3:29092"
run() { echo "### $*"; docker exec kafka-tools timeout 30 "$@" 2>&1 | grep -v -E "WARN|Connection to node" ; echo; }
{
  echo "# $LABEL @ $(date '+%Y-%m-%dT%H:%M:%S')"
  echo "### docker ps"; docker ps --filter name=kafka --format '{{.Names}}\t{{.Status}}'; echo
  run kafka-topics --bootstrap-server "$BS" --describe --topic topic_producer,topic_consumer,topic_consumer_2
  run kafka-metadata-quorum --bootstrap-server "$BS" describe --status
  run kafka-consumer-groups --bootstrap-server "$BS" --describe --state --group kafka-test-service-consumer-1
  run kafka-consumer-groups --bootstrap-server "$BS" --describe --state --group kafka-test-service-consumer-2
  run kafka-consumer-groups --bootstrap-server "$BS" --describe --group kafka-test-service-consumer-1
  run kafka-consumer-groups --bootstrap-server "$BS" --describe --group kafka-test-service-consumer-2
} | tee "$OUT"
