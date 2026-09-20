#!/bin/bash

BROKERS=("kafka-broker-1" "kafka-broker-2")
LOG_DIR="/tmp/kafka-logs"

RED='\033[31m'
RESET='\033[0m'

echo
echo "======================================================================"
echo "                    KAFKA STORAGE UTILIZATION"
echo "======================================================================"
echo

CLUSTER_TOTAL_KB=0
SUMMARY_FILE=$(mktemp)

for BROKER in "${BROKERS[@]}"; do

    echo -e "${RED}BROKER: $BROKER${RESET}"
    echo "----------------------------------------------------------------------"
    printf "%-35s %-12s\n" "TOPIC / PARTITION" "SIZE"
    printf "%-35s %-12s\n" "-----------------------------------" "------------"

    OUTPUT=$(docker exec "$BROKER" sh -c "du -sk $LOG_DIR/* 2>/dev/null")

    while read -r SIZE_KB FILE_PATH; do

        NAME="${FILE_PATH##*/}"

        if [ -n "$NAME" ]; then
            SIZE_H=$(docker exec "$BROKER" du -sh "$FILE_PATH" | cut -f1)

            printf "%-35s %-12s\n" "$NAME" "$SIZE_H"

            # Only application topics go into topic summary
            case "$NAME" in
                order-*)
                    TOPIC="${NAME%-*}"
                    echo "$BROKER|$TOPIC|$SIZE_KB" >> "$SUMMARY_FILE"
                    ;;
            esac
        fi

    done <<< "$OUTPUT"

    BROKER_TOTAL_KB=$(docker exec "$BROKER" du -sk "$LOG_DIR" | cut -f1)
    BROKER_TOTAL=$(docker exec "$BROKER" du -sh "$LOG_DIR" | cut -f1)

    CLUSTER_TOTAL_KB=$((CLUSTER_TOTAL_KB + BROKER_TOTAL_KB))

    echo
    echo -e "${RED}BROKER TOTAL                         $BROKER_TOTAL${RESET}"
    echo
    echo "======================================================================"

done

echo
echo
echo "                           TOPIC SUMMARY"
echo "----------------------------------------------------------------------"
printf "%-35s %-12s %-12s %-12s\n" \
    "TOPIC" "BROKER-1" "BROKER-2" "TOTAL"

printf "%-35s %-12s %-12s %-12s\n" \
    "-----------------------------------" "------------" "------------" "------------"

for TOPIC in $(cut -d'|' -f2 "$SUMMARY_FILE" | sort -u); do

    BROKER1_KB=$(awk -F'|' -v topic="$TOPIC" \
        '$1=="kafka-broker-1" && $2==topic {sum+=$3} END {print sum+0}' \
        "$SUMMARY_FILE")

    BROKER2_KB=$(awk -F'|' -v topic="$TOPIC" \
        '$1=="kafka-broker-2" && $2==topic {sum+=$3} END {print sum+0}' \
        "$SUMMARY_FILE")

    TOTAL_KB=$((BROKER1_KB + BROKER2_KB))

    if [ "$BROKER1_KB" -eq 0 ]; then
        BROKER1_SIZE="-"
    else
        BROKER1_SIZE=$(awk -v kb="$BROKER1_KB" 'BEGIN {
            if (kb >= 1024*1024)
                printf "%.2fG", kb/(1024*1024)
            else if (kb >= 1024)
                printf "%.0fM", kb/1024
            else
                printf "%dK", kb
        }')
    fi

    if [ "$BROKER2_KB" -eq 0 ]; then
        BROKER2_SIZE="-"
    else
        BROKER2_SIZE=$(awk -v kb="$BROKER2_KB" 'BEGIN {
            if (kb >= 1024*1024)
                printf "%.2fG", kb/(1024*1024)
            else if (kb >= 1024)
                printf "%.0fM", kb/1024
            else
                printf "%dK", kb
        }')
    fi

    TOTAL_SIZE=$(awk -v kb="$TOTAL_KB" 'BEGIN {
        if (kb >= 1024*1024)
            printf "%.2fG", kb/(1024*1024)
        else if (kb >= 1024)
            printf "%.0fM", kb/1024
        else
            printf "%dK", kb
    }')

    printf "%-35s %-12s %-12s %-12s\n" \
        "$TOPIC" "$BROKER1_SIZE" "$BROKER2_SIZE" "$TOTAL_SIZE"

done

echo "----------------------------------------------------------------------"

CLUSTER_TOTAL_SIZE=$(awk -v kb="$CLUSTER_TOTAL_KB" 'BEGIN {
    if (kb >= 1024*1024)
        printf "%.2fG", kb/(1024*1024)
    else
        printf "%.0fM", kb/1024
}')

echo -e "${RED}CLUSTER TOTAL                       $CLUSTER_TOTAL_SIZE${RESET}"

echo "======================================================================"

rm -f "$SUMMARY_FILE"