#!/bin/bash
# =============================================================================
# Reset and Run - Iceberg Incremental Demo
# =============================================================================
# Complete reset and fresh start for the Iceberg incremental demo.
#
# Architecture:
#   1. Webhooks → Ingestion API → Kafka (Redpanda)
#   2. Flink SQL jobs read Kafka → write to raw Iceberg tables
#   3. Airflow triggers Spark batch jobs:
#      - staging_batch.py: raw → staging
#      - analytics_incremental.py: staging/core → analytics
#      - marts_incremental.py: analytics → marts
#
# Usage:
#   ./scripts/reset_and_run.sh              # Full reset + run
#   ./scripts/reset_and_run.sh --no-reset   # Skip reset, just run pipeline
#   ./scripts/reset_and_run.sh --reset-only # Only reset infrastructure
#   ./scripts/reset_and_run.sh --help       # Show help
#
# Environment variables:
#   SHOPIFY_CUSTOMERS   Number of Shopify customers (default: 50)
#   SHOPIFY_ORDERS      Number of Shopify orders (default: 100)
#   etc.
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
NC='\033[0m'

# Parse arguments
SKIP_RESET=false
RESET_ONLY=false

for arg in "$@"; do
    case $arg in
        --no-reset)    SKIP_RESET=true ;;
        --reset-only)  RESET_ONLY=true ;;
        --help|-h)
            echo "Iceberg Incremental Demo - Reset and Run"
            echo ""
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --no-reset    Skip reset, just run the pipeline"
            echo "  --reset-only  Only reset, don't run the pipeline"
            echo "  --help, -h    Show this help"
            exit 0
            ;;
    esac
done

log_step() { echo -e "\n${BLUE}══════════════════════════════════════════════════════════${NC}"; echo -e "${BLUE}  $1${NC}"; echo -e "${BLUE}══════════════════════════════════════════════════════════${NC}\n"; }
log_success() { echo -e "${GREEN}✓ $1${NC}"; }
log_warning() { echo -e "${YELLOW}⚠ $1${NC}"; }
log_error() { echo -e "${RED}✗ $1${NC}"; }

wait_for_service() {
    local service=$1
    local max_attempts=${2:-30}
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

# =============================================================================
# PHASE 0: Reset
# =============================================================================
reset_environment() {
    log_step "PHASE 0: Resetting Environment"

    cd "$INFRA_DIR"

    echo "Stopping all services..."
    docker-compose down --remove-orphans 2>/dev/null || true

    echo "Removing data volumes..."
    docker volume rm iceberg-demo-minio-data 2>/dev/null || true
    docker volume rm iceberg-demo-redpanda-data 2>/dev/null || true
    docker volume rm iceberg-demo-flink-checkpoints 2>/dev/null || true
    docker volume rm iceberg-demo-spark-events 2>/dev/null || true
    docker volume rm iceberg-demo-clickhouse-data 2>/dev/null || true
    docker volume rm iceberg-demo-trino-data 2>/dev/null || true
    docker volume rm iceberg-demo-airflow-postgres-data 2>/dev/null || true

    docker container prune -f 2>/dev/null || true

    log_success "Environment reset complete"
}

# =============================================================================
# PHASE 1: Start Infrastructure
# =============================================================================
start_infrastructure() {
    log_step "PHASE 1: Starting Infrastructure"

    cd "$INFRA_DIR"

    echo "Building and starting all services..."
    docker-compose up -d --build

    echo ""
    echo "Waiting for services to be healthy..."
    wait_for_service minio 30 || exit 1
    wait_for_service airflow-postgres 30 || exit 1
    wait_for_service iceberg-rest 30 || exit 1
    wait_for_service redpanda 30 || exit 1
    wait_for_service flink-jobmanager 30 || exit 1
    wait_for_service spark-master 30 || exit 1
    wait_for_service ingestion-api 30 || exit 1
    wait_for_service airflow-scheduler 60 || exit 1

    log_success "All services running"

    echo ""
    echo "  Service URLs:"
    echo "  ─────────────────────────────────────────"
    echo "  Airflow:          http://localhost:8086 (admin/admin123)"
    echo "  Spark Master:     http://localhost:8084"
    echo "  Flink:            http://localhost:8083"
    echo "  MinIO Console:    http://localhost:9001 (admin/admin123456)"
    echo "  Redpanda Console: http://localhost:8080"
    echo "  Ingestion API:    http://localhost:8090"
}

# =============================================================================
# PHASE 2: Initialize Iceberg Catalog
# =============================================================================
init_iceberg_catalog() {
    log_step "PHASE 2: Initializing Iceberg Catalog"

    cd "$INFRA_DIR"

    echo "Creating raw and staging databases..."

    # Submit Flink SQL to create databases
    docker exec iceberg-flink-jobmanager /opt/flink/bin/sql-client.sh embedded -e "
        CREATE CATALOG iceberg_catalog WITH (
            'type' = 'iceberg',
            'catalog-type' = 'rest',
            'uri' = 'http://iceberg-rest:8181',
            'warehouse' = 's3a://warehouse/',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint' = 'http://minio:9000',
            's3.path-style-access' = 'true',
            's3.access-key-id' = 'admin',
            's3.secret-access-key' = 'admin123456'
        );
        USE CATALOG iceberg_catalog;
        CREATE DATABASE IF NOT EXISTS raw COMMENT 'Raw webhook events';
        CREATE DATABASE IF NOT EXISTS staging COMMENT 'Cleaned staging data';
        CREATE DATABASE IF NOT EXISTS semantic COMMENT 'Entity resolution';
        CREATE DATABASE IF NOT EXISTS core COMMENT 'Core business entities';
        CREATE DATABASE IF NOT EXISTS analytics COMMENT 'Analytics metrics';
        CREATE DATABASE IF NOT EXISTS marts COMMENT 'Data marts';
    " 2>/dev/null || log_warning "Some databases may already exist"

    log_success "Iceberg catalog initialized"
}

# =============================================================================
# PHASE 3: Generate Mock Data
# =============================================================================
generate_mock_data() {
    log_step "PHASE 3: Generating Mock Data"

    cd "$PROJECT_DIR"

    # Check if dependencies are available
    if ! python3 -c "import click, httpx, faker" 2>/dev/null; then
        echo "Installing Python dependencies..."
        # Try different installation methods
        if pip3 install --user -q click httpx faker 2>/dev/null; then
            log_success "Dependencies installed with --user"
        elif command -v uv &>/dev/null; then
            uv pip install click httpx faker 2>/dev/null || true
        else
            log_warning "Could not install dependencies automatically"
            echo "  Please run: pip3 install click httpx faker"
            echo "  Or: python3 -m venv .venv && source .venv/bin/activate && pip install click httpx faker"
        fi
    fi

    echo "Posting mock data to ingestion API..."
    echo "  Shopify: $SHOPIFY_CUSTOMERS customers, $SHOPIFY_ORDERS orders"
    echo "  Stripe:  $STRIPE_CUSTOMERS customers, $STRIPE_CHARGES charges"
    echo "  HubSpot: $HUBSPOT_CONTACTS contacts"
    echo ""

    python3 scripts/post_mock_data.py \
        --url http://localhost:8090 \
        --shopify-customers "$SHOPIFY_CUSTOMERS" \
        --shopify-orders "$SHOPIFY_ORDERS" \
        --stripe-customers "$STRIPE_CUSTOMERS" \
        --stripe-charges "$STRIPE_CHARGES" \
        --hubspot-contacts "$HUBSPOT_CONTACTS" \
        --seed 42

    log_success "Mock data generated"
}

# =============================================================================
# PHASE 4: Submit Flink Streaming Jobs
# =============================================================================
submit_flink_jobs() {
    log_step "PHASE 4: Submitting Flink Streaming Jobs"

    cd "$INFRA_DIR"

    echo "Submitting Flink SQL streaming jobs..."
    echo "  These jobs read from Kafka and write to Iceberg raw tables"
    echo ""

    # Submit each ingestion job
    for job in shopify_orders shopify_customers stripe_charges hubspot_contacts; do
        job_file="/opt/flink/jobs/${job}_full.sql"
        echo "  Submitting: $job..."
        docker exec iceberg-flink-jobmanager /opt/flink/bin/sql-client.sh embedded \
            -f "$job_file" 2>/dev/null &
        sleep 2
    done

    echo ""
    log_warning "Flink jobs submitted in background"
    echo "  Monitor at: http://localhost:8083"
    echo "  Waiting 60s for initial data processing..."
    sleep 60

    log_success "Flink streaming jobs running"
}

# =============================================================================
# PHASE 5: Verify Raw Tables
# =============================================================================
verify_raw_tables() {
    log_step "PHASE 5: Verifying Raw Tables"

    cd "$INFRA_DIR"

    echo "Checking Iceberg catalog for raw tables..."

    local table_count=$(docker exec iceberg-airflow-postgres psql -U airflow -d iceberg_catalog -t -c \
        "SELECT COUNT(*) FROM iceberg_tables WHERE table_namespace = 'raw';" 2>/dev/null | tr -d ' ')

    if [ "${table_count:-0}" -ge 4 ]; then
        log_success "Found $table_count raw tables in Iceberg"
    else
        log_warning "Only found ${table_count:-0} raw tables (expected 4)"
        echo "  Raw tables may still be creating. Check Flink UI."
    fi

    # List tables
    echo ""
    echo "  Tables in PostgreSQL catalog:"
    docker exec iceberg-airflow-postgres psql -U airflow -d iceberg_catalog -c \
        "SELECT table_namespace, table_name FROM iceberg_tables ORDER BY table_namespace, table_name;" 2>/dev/null || true
}

# =============================================================================
# PHASE 6: Trigger Airflow DAG
# =============================================================================
trigger_airflow_dag() {
    log_step "PHASE 6: Triggering Airflow DAG"

    cd "$INFRA_DIR"

    echo "Triggering clgraph_iceberg_pipeline DAG..."
    docker exec iceberg-airflow-scheduler airflow dags trigger clgraph_iceberg_pipeline 2>/dev/null || {
        log_error "Failed to trigger DAG"
        echo "  Check Airflow UI: http://localhost:8086"
        return 1
    }

    log_success "DAG triggered"
    echo ""
    echo "  Monitor at: http://localhost:8086/dags/clgraph_iceberg_pipeline/grid"
}

# =============================================================================
# Main
# =============================================================================
main() {
    echo ""
    echo "╔══════════════════════════════════════════════════════════╗"
    echo "║     Iceberg Incremental Demo - Reset and Run             ║"
    echo "╚══════════════════════════════════════════════════════════╝"
    echo ""

    if [ "$SKIP_RESET" = false ]; then
        reset_environment
    else
        log_warning "Skipping reset (--no-reset)"
    fi

    if [ "$RESET_ONLY" = true ]; then
        log_success "Reset complete (--reset-only)"
        exit 0
    fi

    start_infrastructure
    init_iceberg_catalog
    generate_mock_data
    submit_flink_jobs
    verify_raw_tables
    trigger_airflow_dag

    echo ""
    echo "╔══════════════════════════════════════════════════════════╗"
    echo "║                    Demo Setup Complete!                   ║"
    echo "╚══════════════════════════════════════════════════════════╝"
    echo ""
    echo "  Next steps:"
    echo "  ───────────────────────────────────────────────────────────"
    echo "  1. Open Airflow: http://localhost:8086 (admin/admin123)"
    echo "  2. Watch DAG: clgraph_iceberg_pipeline"
    echo "  3. Query data: docker exec -it iceberg-trino trino"
    echo ""
    echo "  Troubleshooting:"
    echo "  ───────────────────────────────────────────────────────────"
    echo "  - If staging fails: Check raw tables exist (Flink jobs)"
    echo "  - If catalog errors: PostgreSQL catalog is used (not SQLite)"
    echo "  - Logs: docker-compose logs -f <service>"
    echo ""
}

main "$@"
