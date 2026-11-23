#!/bin/bash
# ==============================================================================
# APACHE KAFKA CLI DEPLOYMENT & COMMANDS SCRIPT
# TOPICS, PRODUCER & CONSUMER CONSOLE CLI
# ==============================================================================

KAFKA_HOME="/opt/kafka"
BOOTSTRAP_SERVER="localhost:9092"

# 1. Start KRaft-based Kafka Broker (Kafka 3.x+)
echo "=== Generating Storage UUID and Formatting Storage Directory ==="
KAFKA_CLUSTER_ID="$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)"
$KAFKA_HOME/bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c $KAFKA_HOME/config/kraft/server.properties

echo "=== Starting Kafka Broker in KRaft mode ==="
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/kraft/server.properties

# 2. Create Ingestion Topic with 3 Partitions and Replication Factor 1
echo "=== Creating Topic: smartcity-traffic-sensors ==="
$KAFKA_HOME/bin/kafka-topics.sh --create \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --replication-factor 1 \
    --partitions 3 \
    --topic smartcity-traffic-sensors \
    --config retention.ms=86400000 \
    --config cleanup.policy=delete

# 3. Create Alerts Topic
echo "=== Creating Topic: traffic-congestion-alerts ==="
$KAFKA_HOME/bin/kafka-topics.sh --create \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --replication-factor 1 \
    --partitions 3 \
    --topic traffic-congestion-alerts

# 4. Describe Topic Partitions
echo "=== Describing Topic Details ==="
$KAFKA_HOME/bin/kafka-topics.sh --describe \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --topic smartcity-traffic-sensors

# 5. CLI Console Consumer Verification
echo "=== Launching Console Consumer for Real-Time Validation ==="
# $KAFKA_HOME/bin/kafka-console-consumer.sh \
#     --bootstrap-server $BOOTSTRAP_SERVER \
#     --topic traffic-congestion-alerts \
#     --from-beginning \
#     --property print.key=true \
#     --property print.timestamp=true
