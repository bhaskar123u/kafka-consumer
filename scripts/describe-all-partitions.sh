#!/bin/bash

BROKERS=("kafka-broker-1" "kafka-broker-2")

# Find a reachable broker
for BROKER in "${BROKERS[@]}"; do
    if docker exec "$BROKER" \
        /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "$BROKER:9092" \
        --list >/dev/null 2>&1; then

        BOOTSTRAP="$BROKER:9092"
        break
    fi
done

if [ -z "$BOOTSTRAP" ]; then
    echo "No Kafka broker is reachable."
    exit 1
fi

echo "Using broker: $BOOTSTRAP"
echo

# Get all topics
TOPICS=$(docker exec "${BOOTSTRAP%%:*}" \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --list)

for TOPIC in $TOPICS; do

    echo "========================================================"
    echo "TOPIC: $TOPIC"
    echo "========================================================"

    docker exec "${BOOTSTRAP%%:*}" \
        /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "$BOOTSTRAP" \
        --describe \
        --topic "$TOPIC"

    echo
done