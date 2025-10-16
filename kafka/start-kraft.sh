#!/bin/bash
set -e

if [ -z "$KAFKA_CLUSTER_ID" ]; then
  export KAFKA_CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
fi

bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
bin/kafka-server-start.sh config/kraft/server.properties
