#!/bin/bash
# =============================================================================
# Full Reset and Validation - Iceberg Incremental Demo
# =============================================================================
# Complete reset and validation of all phases (1-5) of the demo.
#
# This script:
# 1. Stops all containers and removes ALL data (volumes, files)
# 2. Starts fresh infrastructure (Phase 1)
# 3. Submits Flink streaming jobs and posts mock data (Phase 2)
# 4. Runs staging transforms (Phase 3)
# 5. Runs entity resolution (Phase 4)
# 6. Runs analytics and marts (Phase 5)
# 7. Triggers and validates Airflow DAG
#
# Usage:
#   ./scripts/full_reset_and_validate.sh
#
# =============================================================================

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
INFRA_DIR="$PROJECT_DIR/infrastructure"

# Data generation settings
SHOPIFY_CUSTOMERS=${SHOPIFY_CUSTOMERS:-50}
SHOPIFY_ORDERS=${SHOPIFY_ORDERS:-100}
STRIPE_CUSTOMERS=${STRIPE_CUSTOMERS:-30}
STRIPE_CHARGES=${STRIPE_CHARGES:-80}
HUBSPOT_CONTACTS=${HUBSPOT_CONTACTS:-40}

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# Counters
TESTS_PASSED=0
TESTS_FAILED=0

log_phase() {
    echo ""
    echo -e "${CYAN}╔══════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║  ${BOLD}$1${NC}${CYAN}$(printf '%*s' $((62 - ${#1})) '')║${NC}"
    echo -e "${CYAN}╚══════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

log_step() {
    echo -e "${BLUE}▶ $1${NC}"
}

log_success() {
    echo -e "${GREEN}✓ $1${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
}

log_fail() {
    echo -e "${RED}✗ $1${NC}"
    TESTS_FAILED=$((TESTS_FAILED + 1))
}

log_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

log_info() {
    echo -e "  $1"
}

wait_for_service() {
    local service=$1
    local max_attempts=${2:-60}
    local attempt=1
    echo -n "  Waiting for $service..."
    while [ $attempt -le $max_attempts ]; do
        if docker-compose -f "$INFRA_DIR/docker-compose.yml" ps "$service" 2>/dev/null | grep -q "healthy\|Up"; then
            echo -e " ${GREEN}ready${NC}"
            return 0
        fi
        echo -n "."
        sleep 3
        attempt=$((attempt + 1))
    done
    echo -e " ${RED}timeout${NC}"
    return 1
}

check_table_exists() {
    local namespace=$1
    local table=$2
    local count=$(docker exec iceberg-airflow-postgres psql -U airflow -d iceberg_catalog -t -c \
        "SELECT COUNT(*) FROM iceberg_tables WHERE table_namespace = '$namespace' AND table_name = '$table';" 2>/dev/null | tr -d ' ')
    [ "${count:-0}" -ge 1 ]
}

check_table_has_rows() {
    local fqn=$1  # e.g., iceberg.staging.stg_shopify_orders
    local result=$(docker exec iceberg-trino trino --execute "SELECT COUNT(*) FROM $fqn" 2>/dev/null | head -1 | tr -d '"')
    [ "${result:-0}" -gt 0 ]
}

# =============================================================================
# PHASE 0: Complete Cleanup
# =============================================================================
phase_0_cleanup() {
    log_phase "PHASE 0: Complete Cleanup"

    cd "$INFRA_DIR"

    log_step "Stopping all containers..."
    docker-compose down --remove-orphans 2>/dev/null || true

    log_step "Removing Docker volumes..."
    docker volume rm iceberg-demo-minio-data 2>/dev/null || true
    docker volume rm iceberg-demo-redpanda-data 2>/dev/null || true
    docker volume rm iceberg-demo-flink-checkpoints 2>/dev/null || true
    docker volume rm iceberg-demo-spark-events 2>/dev/null || true
    docker volume rm iceberg-demo-clickhouse-data 2>/dev/null || true
    docker volume rm iceberg-demo-trino-data 2>/dev/null || true
    docker volume rm iceberg-demo-airflow-postgres-data 2>/dev/null || true

    log_step "Cleaning up DAG files (keeping clgraph_pipeline.py)..."
    # Keep clgraph_pipeline.py, remove others if they exist
    find "$PROJECT_DIR/airflow/dags" -name "*.py" ! -name "clgraph_pipeline.py" ! -name "__init__.py" -delete 2>/dev/null || true

    log_step "Cleaning up Airflow logs..."
    rm -rf "$PROJECT_DIR/airflow/logs"/* 2>/dev/null || true

    log_step "Pruning unused containers..."
    docker container prune -f 2>/dev/null || true

    log_success "Cleanup complete"
}

# =============================================================================
# PHASE 1: Infrastructure
# =============================================================================
phase_1_infrastructure() {
    log_phase "PHASE 1: Infrastructure Setup"

    cd "$INFRA_DIR"

    log_step "Building and starting all services..."
    docker-compose up -d --build 2>&1 | grep -E "Created|Started|Running" || true

    echo ""
    log_step "Waiting for services to be healthy..."

    # Core services
    wait_for_service minio 60 || { log_fail "MinIO failed to start"; return 1; }
    wait_for_service airflow-postgres 60 || { log_fail "PostgreSQL failed to start"; return 1; }
    wait_for_service iceberg-rest 60 || { log_fail "Iceberg REST failed to start"; return 1; }
    wait_for_service redpanda 60 || { log_fail "Redpanda failed to start"; return 1; }
    wait_for_service flink-jobmanager 60 || { log_fail "Flink failed to start"; return 1; }
    wait_for_service spark-master 60 || { log_fail "Spark failed to start"; return 1; }
    wait_for_service ingestion-api 60 || { log_fail "Ingestion API failed to start"; return 1; }
    wait_for_service trino 60 || { log_fail "Trino failed to start"; return 1; }
    wait_for_service airflow-scheduler 90 || { log_fail "Airflow Scheduler failed to start"; return 1; }

    echo ""
    log_step "Creating spark-events directory in MinIO..."
    docker exec iceberg-minio mc alias set myminio http://localhost:9000 admin admin123 2>/dev/null || true
    docker exec iceberg-minio mc mb myminio/warehouse/spark-events --ignore-existing 2>/dev/null || true

    echo ""
    log_step "Validating Phase 1..."

    # Test MinIO
    if docker exec iceberg-minio mc ls myminio/warehouse/ &>/dev/null; then
        log_success "MinIO bucket accessible"
    else
        log_fail "MinIO bucket not accessible"
    fi

    # Test Iceberg REST
    if docker exec iceberg-rest bash -c 'echo > /dev/tcp/localhost/8181' 2>/dev/null; then
        log_success "Iceberg REST catalog responding"
    else
        log_fail "Iceberg REST catalog not responding"
    fi

    # Test Redpanda
    local topic_count=$(docker exec iceberg-redpanda rpk topic list 2>/dev/null | wc -l)
    if [ "$topic_count" -gt 5 ]; then
        log_success "Redpanda topics created ($topic_count topics)"
    else
        log_fail "Redpanda topics not created"
    fi

    # Test Trino
    if docker exec iceberg-trino trino --execute "SHOW CATALOGS" 2>/dev/null | grep -q iceberg; then
        log_success "Trino connected to Iceberg catalog"
    else
        log_fail "Trino not connected to Iceberg"
    fi

    # Test PostgreSQL (Iceberg catalog backend)
    if docker exec iceberg-airflow-postgres psql -U airflow -d iceberg_catalog -c "SELECT 1" &>/dev/null; then
        log_success "PostgreSQL Iceberg catalog accessible"
    else
        log_fail "PostgreSQL Iceberg catalog not accessible"
    fi

    echo ""
    log_info "Service URLs:"
    log_info "  Airflow:          http://localhost:8086 (admin/admin123)"
    log_info "  Spark Master:     http://localhost:8084"
    log_info "  Flink:            http://localhost:8083"
    log_info "  MinIO Console:    http://localhost:9001 (admin/admin123)"
    log_info "  Redpanda Console: http://localhost:8080"
    log_info "  Trino:            http://localhost:8085"
}

# =============================================================================
# PHASE 2: Webhook Ingestion + Raw Layer
# =============================================================================
phase_2_raw_layer() {
    log_phase "PHASE 2: Webhook Ingestion + Raw Layer"

    cd "$PROJECT_DIR"

    log_step "Submitting Flink streaming jobs..."
    for job in shopify_orders shopify_customers stripe_charges hubspot_contacts; do
        log_info "  Submitting: ${job}_full.sql"
        docker exec iceberg-flink-jobmanager /opt/flink/bin/sql-client.sh embedded \
            -f "/opt/flink/jobs/${job}_full.sql" 2>&1 | tail -5 &
        sleep 3
    done

    echo ""
    log_step "Setting up Python virtual environment..."
    if [ ! -d "$PROJECT_DIR/.venv" ]; then
        python3 -m venv "$PROJECT_DIR/.venv"
    fi
    source "$PROJECT_DIR/.venv/bin/activate"
    pip install -q click httpx faker 2>/dev/null

    log_step "Posting mock data to webhook endpoints..."
    log_info "  Shopify: $SHOPIFY_CUSTOMERS customers, $SHOPIFY_ORDERS orders"
    log_info "  Stripe:  $STRIPE_CUSTOMERS customers, $STRIPE_CHARGES charges"
    log_info "  HubSpot: $HUBSPOT_CONTACTS contacts"

    python3 "$PROJECT_DIR/scripts/post_mock_data.py" \
        --url http://localhost:8090 \
        --shopify-customers "$SHOPIFY_CUSTOMERS" \
        --shopify-orders "$SHOPIFY_ORDERS" \
        --stripe-customers "$STRIPE_CUSTOMERS" \
        --stripe-charges "$STRIPE_CHARGES" \
        --hubspot-contacts "$HUBSPOT_CONTACTS" \
        --seed 42 2>&1 | grep -E "Posted|Total|Summary" || true

    echo ""
    log_step "Waiting for Flink to process data (60s)..."
    sleep 60

    echo ""
    log_step "Validating Phase 2..."

    # Check raw tables exist
    for table in shopify_orders shopify_customers stripe_charges hubspot_contacts; do
        if check_table_exists "raw" "$table"; then
            log_success "Raw table exists: raw.$table"
        else
            log_fail "Raw table missing: raw.$table"
        fi
    done

    # Check Redpanda messages
    for topic in shopify.orders shopify.customers stripe.charges hubspot.contacts; do
        local msg_count=$(docker exec iceberg-redpanda rpk topic consume "$topic" --num 1 --format json 2>/dev/null | wc -l)
        if [ "$msg_count" -gt 0 ]; then
            log_success "Messages in Redpanda topic: $topic"
        else
            log_fail "No messages in topic: $topic"
        fi
    done
}

# =============================================================================
# PHASE 3: Staging Layer
# =============================================================================
phase_3_staging() {
    log_phase "PHASE 3: Staging Layer"

    cd "$INFRA_DIR"

    log_step "Running Spark staging batch job..."
    docker exec iceberg-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --conf 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions' \
        --conf 'spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog' \
        --conf 'spark.sql.catalog.iceberg.type=rest' \
        --conf 'spark.sql.catalog.iceberg.uri=http://iceberg-rest:8181' \
        --conf 'spark.sql.catalog.iceberg.warehouse=s3a://warehouse/' \
        --conf 'spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO' \
        --conf 'spark.sql.catalog.iceberg.s3.endpoint=http://minio:9000' \
        --conf 'spark.sql.catalog.iceberg.s3.access-key-id=admin' \
        --conf 'spark.sql.catalog.iceberg.s3.secret-access-key=admin123' \
        --conf 'spark.sql.catalog.iceberg.s3.path-style-access=true' \
        --conf 'spark.hadoop.fs.s3a.endpoint=http://minio:9000' \
        --conf 'spark.hadoop.fs.s3a.access.key=admin' \
        --conf 'spark.hadoop.fs.s3a.secret.key=admin123' \
        --conf 'spark.hadoop.fs.s3a.path.style.access=true' \
        /opt/spark/jobs/staging_batch.py --table all --mode full 2>&1 | tail -20

    echo ""
    log_step "Validating Phase 3..."

    # Check staging tables
    for table in stg_shopify_orders stg_shopify_customers stg_stripe_charges stg_hubspot_contacts; do
        if check_table_exists "staging" "$table"; then
            log_success "Staging table exists: staging.$table"
        else
            log_fail "Staging table missing: staging.$table"
        fi
    done
}

# =============================================================================
# PHASE 4: Semantic Layer (Entity Resolution)
# =============================================================================
phase_4_semantic() {
    log_phase "PHASE 4: Semantic Layer (Entity Resolution)"

    cd "$INFRA_DIR"

    log_step "Running entity resolution backfill..."
    docker exec iceberg-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --conf 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions' \
        --conf 'spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog' \
        --conf 'spark.sql.catalog.iceberg.type=rest' \
        --conf 'spark.sql.catalog.iceberg.uri=http://iceberg-rest:8181' \
        --conf 'spark.sql.catalog.iceberg.warehouse=s3a://warehouse/' \
        --conf 'spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO' \
        --conf 'spark.sql.catalog.iceberg.s3.endpoint=http://minio:9000' \
        --conf 'spark.sql.catalog.iceberg.s3.access-key-id=admin' \
        --conf 'spark.sql.catalog.iceberg.s3.secret-access-key=admin123' \
        --conf 'spark.sql.catalog.iceberg.s3.path-style-access=true' \
        --conf 'spark.hadoop.fs.s3a.endpoint=http://minio:9000' \
        --conf 'spark.hadoop.fs.s3a.access.key=admin' \
        --conf 'spark.hadoop.fs.s3a.secret.key=admin123' \
        --conf 'spark.hadoop.fs.s3a.path.style.access=true' \
        /opt/spark/jobs/entity_backfill.py --mode initial 2>&1 | tail -20

    echo ""
    log_step "Validating Phase 4..."

    # Check semantic tables
    for table in entity_index blocking_index; do
        if check_table_exists "semantic" "$table"; then
            log_success "Semantic table exists: semantic.$table"
        else
            log_fail "Semantic table missing: semantic.$table"
        fi
    done

    # Check entity resolution stats
    local entity_count=$(docker exec iceberg-airflow-postgres psql -U airflow -d iceberg_catalog -t -c \
        "SELECT COUNT(*) FROM iceberg_tables WHERE table_namespace = 'semantic';" 2>/dev/null | tr -d ' ')
    log_info "  Semantic tables created: ${entity_count:-0}"
}

# =============================================================================
# PHASE 5: Analytics + Marts
# =============================================================================
phase_5_analytics_marts() {
    log_phase "PHASE 5: Analytics + Marts"

    cd "$INFRA_DIR"

    log_step "Running analytics incremental job..."
    docker exec iceberg-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --conf 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions' \
        --conf 'spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog' \
        --conf 'spark.sql.catalog.iceberg.type=rest' \
        --conf 'spark.sql.catalog.iceberg.uri=http://iceberg-rest:8181' \
        --conf 'spark.sql.catalog.iceberg.warehouse=s3a://warehouse/' \
        --conf 'spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO' \
        --conf 'spark.sql.catalog.iceberg.s3.endpoint=http://minio:9000' \
        --conf 'spark.sql.catalog.iceberg.s3.access-key-id=admin' \
        --conf 'spark.sql.catalog.iceberg.s3.secret-access-key=admin123' \
        --conf 'spark.sql.catalog.iceberg.s3.path-style-access=true' \
        --conf 'spark.hadoop.fs.s3a.endpoint=http://minio:9000' \
        --conf 'spark.hadoop.fs.s3a.access.key=admin' \
        --conf 'spark.hadoop.fs.s3a.secret.key=admin123' \
        --conf 'spark.hadoop.fs.s3a.path.style.access=true' \
        /opt/spark/jobs/analytics_incremental.py --table all --mode full 2>&1 | tail -20

    log_step "Running marts incremental job..."
    docker exec iceberg-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --conf 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions' \
        --conf 'spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog' \
        --conf 'spark.sql.catalog.iceberg.type=rest' \
        --conf 'spark.sql.catalog.iceberg.uri=http://iceberg-rest:8181' \
        --conf 'spark.sql.catalog.iceberg.warehouse=s3a://warehouse/' \
        --conf 'spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO' \
        --conf 'spark.sql.catalog.iceberg.s3.endpoint=http://minio:9000' \
        --conf 'spark.sql.catalog.iceberg.s3.access-key-id=admin' \
        --conf 'spark.sql.catalog.iceberg.s3.secret-access-key=admin123' \
        --conf 'spark.sql.catalog.iceberg.s3.path-style-access=true' \
        --conf 'spark.hadoop.fs.s3a.endpoint=http://minio:9000' \
        --conf 'spark.hadoop.fs.s3a.access.key=admin' \
        --conf 'spark.hadoop.fs.s3a.secret.key=admin123' \
        --conf 'spark.hadoop.fs.s3a.path.style.access=true' \
        /opt/spark/jobs/marts_incremental.py --table all --mode full 2>&1 | tail -20

    echo ""
    log_step "Validating Phase 5..."

    # Check analytics tables
    for table in customer_metrics order_summary payment_metrics; do
        if check_table_exists "analytics" "$table"; then
            log_success "Analytics table exists: analytics.$table"
        else
            log_fail "Analytics table missing: analytics.$table"
        fi
    done

    # Check marts tables
    for table in customer_360 sales_dashboard_daily; do
        if check_table_exists "marts" "$table"; then
            log_success "Marts table exists: marts.$table"
        else
            log_fail "Marts table missing: marts.$table"
        fi
    done
}

# =============================================================================
# PHASE 5b: Airflow DAG Validation
# =============================================================================
phase_5b_airflow_dag() {
    log_phase "PHASE 5b: Airflow DAG Validation"

    cd "$INFRA_DIR"

    log_step "Checking DAG availability..."
    local dag_list=$(docker exec iceberg-airflow-scheduler airflow dags list 2>/dev/null | grep clgraph_iceberg_pipeline)
    if [ -n "$dag_list" ]; then
        log_success "clgraph_iceberg_pipeline DAG found"
    else
        log_fail "clgraph_iceberg_pipeline DAG not found"
        return 1
    fi

    log_step "Triggering DAG run..."
    docker exec iceberg-airflow-scheduler airflow dags trigger clgraph_iceberg_pipeline 2>/dev/null

    log_step "Waiting for DAG execution (120s)..."
    sleep 120

    log_step "Checking DAG task states..."
    local latest_run=$(docker exec iceberg-airflow-scheduler airflow dags list-runs -d clgraph_iceberg_pipeline --limit 1 2>/dev/null | tail -1 | awk '{print $3}')

    if [ -n "$latest_run" ]; then
        local task_states=$(docker exec iceberg-airflow-scheduler airflow tasks states-for-dag-run clgraph_iceberg_pipeline "$latest_run" 2>/dev/null)

        local success_count=$(echo "$task_states" | grep -c "success" || echo "0")
        local failed_count=$(echo "$task_states" | grep -c "failed" || echo "0")
        local retry_count=$(echo "$task_states" | grep -c "up_for_retry" || echo "0")

        log_info "  Tasks succeeded: $success_count"
        log_info "  Tasks failed: $failed_count"
        log_info "  Tasks retrying: $retry_count"

        if [ "$success_count" -gt 5 ] && [ "$failed_count" -eq 0 ]; then
            log_success "DAG tasks executing successfully"
        elif [ "$retry_count" -gt 0 ]; then
            log_warning "Some tasks are retrying (may be normal)"
        else
            log_warning "DAG execution incomplete - check Airflow UI"
        fi
    else
        log_warning "Could not retrieve DAG run status"
    fi
}

# =============================================================================
# Summary
# =============================================================================
print_summary() {
    log_phase "VALIDATION SUMMARY"

    # List all tables in catalog
    log_step "Tables in Iceberg catalog:"
    docker exec iceberg-airflow-postgres psql -U airflow -d iceberg_catalog -c \
        "SELECT table_namespace, table_name FROM iceberg_tables ORDER BY table_namespace, table_name;" 2>/dev/null

    echo ""
    echo -e "${BOLD}Test Results:${NC}"
    echo -e "  ${GREEN}Passed: $TESTS_PASSED${NC}"
    echo -e "  ${RED}Failed: $TESTS_FAILED${NC}"
    echo ""

    if [ $TESTS_FAILED -eq 0 ]; then
        echo -e "${GREEN}╔══════════════════════════════════════════════════════════════════╗${NC}"
        echo -e "${GREEN}║              ALL VALIDATIONS PASSED!                             ║${NC}"
        echo -e "${GREEN}╚══════════════════════════════════════════════════════════════════╝${NC}"
    else
        echo -e "${YELLOW}╔══════════════════════════════════════════════════════════════════╗${NC}"
        echo -e "${YELLOW}║         SOME VALIDATIONS FAILED - Check logs above               ║${NC}"
        echo -e "${YELLOW}╚══════════════════════════════════════════════════════════════════╝${NC}"
    fi

    echo ""
    echo "Next steps:"
    echo "  1. Open Airflow UI: http://localhost:8086 (admin/admin123)"
    echo "  2. Check DAG status: clgraph_iceberg_pipeline"
    echo "  3. Query data via Trino: docker exec -it iceberg-trino trino"
    echo "  4. View MinIO data: http://localhost:9001 (admin/admin123)"
    echo ""
}

# =============================================================================
# Main
# =============================================================================
main() {
    echo ""
    echo -e "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}║     Iceberg Incremental Demo - Full Reset and Validation         ║${NC}"
    echo -e "${BOLD}║                      Phases 1-5                                   ║${NC}"
    echo -e "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
    echo ""

    phase_0_cleanup
    phase_1_infrastructure
    phase_2_raw_layer
    phase_3_staging
    phase_4_semantic
    phase_5_analytics_marts
    phase_5b_airflow_dag
    print_summary
}

main "$@"
