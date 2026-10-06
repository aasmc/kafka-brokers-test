#!/usr/bin/env bash
# Creates the three test topics (1 partition, RF=3, min.insync.replicas=2).
# Replica assignment is explicit so the preferred leader is deterministic:
#   topic_producer   -> leader kafka1 (replicas 1,2,3)
#   topic_consumer   -> leader kafka1 (replicas 1,3,2)
#   topic_consumer_2 -> leader kafka2 (replicas 2,3,1)
# kafka1 is the 3rd host in the app's bootstrap list, i.e. the broker that "dies" in the incident.
set -euo pipefail

BS="kafka1:29092,kafka2:29092,kafka3:29092"

create() {
  local topic=$1 assignment=$2
  kafka-topics --bootstrap-server "$BS" --create --if-not-exists \
    --topic "$topic" --replica-assignment "$assignment" \
    --config min.insync.replicas=2
}

create topic_producer   1:2:3
create topic_consumer   1:3:2
create topic_consumer_2 2:3:1

kafka-topics --bootstrap-server "$BS" --describe --topic topic_producer,topic_consumer,topic_consumer_2
