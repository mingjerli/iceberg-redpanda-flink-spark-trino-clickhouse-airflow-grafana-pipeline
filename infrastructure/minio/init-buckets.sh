#!/bin/bash
# =============================================================================
# MinIO Bucket Initialization Script
# =============================================================================
# This script runs after MinIO starts to create required buckets.
# It uses the MinIO Client (mc) to create buckets if they don't exist.
# =============================================================================

set -e

MINIO_HOST="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_USER="${MINIO_ROOT_USER:-admin}"
MINIO_PASS="${MINIO_ROOT_PASSWORD:-admin123}"

# Wait for MinIO to be ready by attempting to configure mc alias
echo "Waiting for MinIO to be ready at ${MINIO_HOST}..."
MAX_RETRIES=30
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if mc alias set local "${MINIO_HOST}" "${MINIO_USER}" "${MINIO_PASS}" > /dev/null 2>&1; then
        echo "MinIO is ready!"
        break
    fi
    echo "MinIO is not ready yet, waiting... (attempt $((RETRY_COUNT + 1))/$MAX_RETRIES)"
    RETRY_COUNT=$((RETRY_COUNT + 1))
    sleep 2
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo "ERROR: MinIO did not become ready in time"
    exit 1
fi

# Create warehouse bucket (for Iceberg tables)
WAREHOUSE_BUCKET="${MINIO_WAREHOUSE_BUCKET:-warehouse}"
if ! mc ls "local/${WAREHOUSE_BUCKET}" > /dev/null 2>&1; then
    echo "Creating bucket: ${WAREHOUSE_BUCKET}"
    mc mb "local/${WAREHOUSE_BUCKET}"
    echo "Bucket ${WAREHOUSE_BUCKET} created successfully"
else
    echo "Bucket ${WAREHOUSE_BUCKET} already exists"
fi

# Create raw-events bucket (for raw JSON archives)
RAW_EVENTS_BUCKET="${MINIO_RAW_EVENTS_BUCKET:-raw-events}"
if ! mc ls "local/${RAW_EVENTS_BUCKET}" > /dev/null 2>&1; then
    echo "Creating bucket: ${RAW_EVENTS_BUCKET}"
    mc mb "local/${RAW_EVENTS_BUCKET}"
    echo "Bucket ${RAW_EVENTS_BUCKET} created successfully"
else
    echo "Bucket ${RAW_EVENTS_BUCKET} already exists"
fi

# Create spark-events directory for Spark event logging
echo "Creating spark-events directory in warehouse bucket..."
# Create an empty object to ensure the directory exists
echo "" | mc pipe "local/${WAREHOUSE_BUCKET}/spark-events/.keep"
echo "spark-events directory created"

# Set bucket policies for public read (optional, useful for debugging)
# Uncomment if you want public read access
# mc anonymous set download "local/${WAREHOUSE_BUCKET}"
# mc anonymous set download "local/${RAW_EVENTS_BUCKET}"

echo "MinIO initialization complete!"
echo "Buckets available:"
mc ls local/
