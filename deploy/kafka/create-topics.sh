#!/bin/bash
# Creates all topics. Idempotent (--if-not-exists), safe to run repeatedly.
#
# Partitions = the unit of parallelism AND ordering:
#   - messages with the same key always land in the same partition (ordered)
#   - one partition is read by at most one consumer of a group, so
#     "number of partitions" is the maximum number of consumer instances
#     that can share the work.
set -euo pipefail

BOOTSTRAP=kafka:29092
create() {
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 1 \
    --config retention.ms="$3"
}

SEVEN_DAYS=604800000
THIRTY_DAYS=2592000000

# Business event streams
create orders.v1   6 "$SEVEN_DAYS"
create payments.v1 6 "$SEVEN_DAYS"

# Dead-letter queues: one per consumer group, kept longer for investigation
create order-service.dlq        1 "$THIRTY_DAYS"
create payment-service.dlq      1 "$THIRTY_DAYS"
create notification-service.dlq 1 "$THIRTY_DAYS"

echo "topics:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
