#!/bin/bash
# =============================================================================
# Redpanda Topic Initialization Script
# =============================================================================
# This script creates the required Kafka topics for the demo.
# It runs after Redpanda is ready.
# =============================================================================

set -e

BROKER="${REDPANDA_BROKER:-redpanda:9092}"
ADMIN="${REDPANDA_ADMIN:-redpanda:9644}"
PARTITIONS="${REDPANDA_DEFAULT_PARTITIONS:-3}"
REPLICATION="${REDPANDA_DEFAULT_REPLICATION:-1}"

# rpk common options to connect to broker and admin API
RPK_OPTS="-X brokers=${BROKER} -X admin.hosts=${ADMIN}"

# Wait for Redpanda to be ready
echo "Waiting for Redpanda at ${BROKER} to be ready..."
MAX_RETRIES=30
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if rpk ${RPK_OPTS} cluster health 2>/dev/null | grep -q "Healthy:.*true"; then
        echo "Redpanda is ready!"
        break
    fi
    echo "Redpanda is not ready yet, waiting... (attempt $((RETRY_COUNT + 1))/$MAX_RETRIES)"
    RETRY_COUNT=$((RETRY_COUNT + 1))
    sleep 3
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo "ERROR: Redpanda did not become ready in time"
    exit 1
fi

# Function to create topic if it doesn't exist
create_topic() {
    local topic_name=$1
    local partitions=${2:-$PARTITIONS}
    local replication=${3:-$REPLICATION}

    if rpk ${RPK_OPTS} topic describe "$topic_name" > /dev/null 2>&1; then
        echo "Topic '$topic_name' already exists"
    else
        echo "Creating topic: $topic_name (partitions=$partitions, replication=$replication)"
        rpk ${RPK_OPTS} topic create "$topic_name" \
            --partitions "$partitions" \
            --replicas "$replication"
        echo "Topic '$topic_name' created successfully"
    fi
}

# -----------------------------------------------------------------------------
# Shopify Topics
# -----------------------------------------------------------------------------
create_topic "shopify.orders" 3
create_topic "shopify.customers" 3
create_topic "shopify.products" 3

# -----------------------------------------------------------------------------
# Stripe Topics
# -----------------------------------------------------------------------------
create_topic "stripe.customers" 3
create_topic "stripe.charges" 3
create_topic "stripe.payment_intents" 3
create_topic "stripe.subscriptions" 3
create_topic "stripe.refunds" 3

# -----------------------------------------------------------------------------
# HubSpot Topics
# -----------------------------------------------------------------------------
create_topic "hubspot.contacts" 3
create_topic "hubspot.companies" 3
create_topic "hubspot.deals" 3

# -----------------------------------------------------------------------------
# Internal Topics (for dead letter queue, etc.)
# -----------------------------------------------------------------------------
create_topic "dlq.ingestion-errors" 1
create_topic "metadata.watermarks" 1

echo ""
echo "Topic initialization complete!"
echo "Available topics:"
rpk ${RPK_OPTS} topic list
