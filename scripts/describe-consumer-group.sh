#!/bin/bash

#this script needs the broker names
BROKERS=("kafka-broker-1" "kafka-broker-2")

for BROKER in "${BROKERS[@]}"; do
    if docker exec "$BROKER" \
        /opt/kafka/bin/kafka-broker-api-versions.sh \
        --bootstrap-server "$BROKER:9092" >/dev/null 2>&1; then

        echo "Using broker: $BROKER"
        BOOTSTRAP_SERVER="$BROKER:9092"
        break
    fi
done

if [ -z "$BOOTSTRAP_SERVER" ]; then
    echo "ERROR: No Kafka broker is reachable."
    exit 1
fi

echo
echo "========================================"
echo "        KAFKA CONSUMER GROUPS"
echo "========================================"

CONSUMER_GROUPS=$(docker exec "$BROKER" \
    /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --list)

for GROUP in $CONSUMER_GROUPS; do

    echo
    echo "----------------------------------------"
    echo "GROUP: $GROUP"
    echo "----------------------------------------"

    docker exec "$BROKER" \
        /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --describe \
        --group "$GROUP" |
    awk '
    NR == 1 {
        printf "\033[31m%s\033[0m\n", $0
        next
    }
    {
        printf "%-20s ", $1
        printf "\033[31m%-15s %-10s %-15s %-15s %-10s\033[0m ", $2, $3, $4, $5, $6
        for (i=7; i<=NF; i++)
            printf "%s%s", $i, (i==NF ? "\n" : " ")
    }'

done