#!/bin/sh
set -eu

for topic in ktm.events.raw.skud ktm.events.raw.cv ktm.events.raw.1c-do; do
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 \
        --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1 \
        --config retention.ms=604800000
done

for topic in ktm.events.rejected ktm.events.dlq; do
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 \
        --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1 \
        --config retention.ms=2592000000
done
