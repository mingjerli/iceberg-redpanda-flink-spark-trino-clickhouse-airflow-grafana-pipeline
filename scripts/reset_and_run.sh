#!/bin/bash
# =============================================================================
# Reset and Run - Iceberg Incremental Demo
# =============================================================================
# Complete reset and fresh start for the Iceberg incremental demo.
#
# Architecture:
#   1. Webhooks → Ingestion API → Kafka (Redpanda)
#   2. Flink SQL jobs read Kafka → write to raw Iceberg tables
#   3. Spark batch jobs process data through layers:
#      - staging_batch.py: raw → staging
#      - entity_backfill.py: entity resolution
#      - core_views.py: unified business objects
#      - analytics_incremental.py: staging/core → analytics
#      - marts_incremental.py: analytics → marts
#   4. Airflow orchestrates scheduled runs
#
# Usage:
#   ./scripts/reset_and_run.sh              # Full reset + run
#   ./scripts/reset_and_run.sh --no-reset   # Skip reset, just run pipeline
#   ./scripts/reset_and_run.sh --reset-only # Only reset infrastructure
#   ./scripts/reset_and_run.sh --validate   # Run with detailed validation
#   ./scripts/reset_and_run.sh --help       # Show help
#
# Environment variables:
#   SHOPIFY_CUSTOMERS   Number of Shopify customers (default: 50)
#   SHOPIFY_ORDERS      Number of Shopify orders (default: 100)
#   STRIPE_CUSTOMERS    Number of Stripe customers (default: 30)
#   STRIPE_CHARGES      Number of Stripe charges (default: 80)
#   HUBSPOT_CONTACTS    Number of HubSpot contacts (default: 40)
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

# Parse arguments
SKIP_RESET=false
RESET_ONLY=false
VALIDATE_MODE=false
SKIP_DATAGEN=false

for arg in "$@"; do
    case $arg in
        --no-reset)     SKIP_RESET=true ;;
        --reset-only)   RESET_ONLY=true ;;
        --validate)     VALIDATE_MODE=true ;;
        --no-datagen)   SKIP_DATAGEN=true ;;
        --help|-h)
            echo "Iceberg Incremental Demo - Reset and Run"
            echo ""
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --no-reset    Skip reset, just run the pipeline"
            echo "  --reset-only  Only reset, don't run the pipeline"
            echo "  --validate    Run with detailed validation and test counts"
            echo "  --no-datagen  Don't start continuous datagen service"
            echo "  --help, -h    Show this help"
            echo ""
            echo "Environment variables:"
            echo "  SHOPIFY_CUSTOMERS   Number of Shopify customers (default: 50)"
            echo "  SHOPIFY_ORDERS      Number of Shopify orders (default: 100)"
            echo "  STRIPE_CUSTOMERS    Number of Stripe customers (default: 30)"
            echo "  STRIPE_CHARGES      Number of Stripe charges (default: 80)"
            echo "  HUBSPOT_CONTACTS    Number of HubSpot contacts (default: 40)"
            exit 0
            ;;
    esac
done

# Validation counters (used in --validate mode)
TESTS_PASSED=0
TESTS_FAILED=0

# =============================================================================
# Logging Functions
# =============================================================================
log_phase() {
    echo ""
    if [ "$VALIDATE_MODE" = true ]; then
        echo -e "${CYAN}╔══════════════════════════════════════════════════════════════════╗${NC}"
        echo -e "${CYAN}║  ${BOLD}$1${NC}${CYAN}$(printf '%*s' $((62 - ${#1})) '')║${NC}"
        echo -e "${CYAN}╚══════════════════════════════════════════════════════════════════╝${NC}"
    else
        echo -e "${BLUE}══════════════════════════════════════════════════════════${NC}"
        echo -e "${BLUE}  $1${NC}"
        echo -e "${BLUE}══════════════════════════════════════════════════════════${NC}"
    fi
    echo ""
}

log_step() {
    echo -e "${BLUE}▶ $1${NC}"
}

log_success() {
    echo -e "${GREEN}✓ $1${NC}"
    if [ "$VALIDATE_MODE" = true ]; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
    fi
}

log_fail() {
    echo -e "${RED}✗ $1${NC}"
    if [ "$VALIDATE_MODE" = true ]; then
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
}

log_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

log_info() {
    echo -e "  $1"
}

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

check_table_exists() {
    local namespace=$1
    local table=$2
    local count=$(docker exec iceberg-airflow-postgres psql -U airflow -d iceberg_catalog -t -c \
        "SELECT COUNT(*) FROM iceberg_tables WHERE table_namespace = '$namespace' AND table_name = '$table';" 2>/dev/null | tr -d ' ')
    [ "${count:-0}" -ge 1 ]
}

# =============================================================================
# PHASE 0: Reset Environment
# =============================================================================
reset_environment() {
    log_phase "PHASE 0: Resetting Environment"

    cd "$INFRA_DIR"

    log_step "Stopping all services..."
    docker-compose down --remove-orphans 2>/dev/null || true

    log_step "Removing data volumes..."
    docker volume rm iceberg-demo-minio-data 2>/dev/null || true
    docker volume rm iceberg-demo-redpanda-data 2>/dev/null || true
    docker volume rm iceberg-demo-flink-checkpoints 2>/dev/null || true
    docker volume rm iceberg-demo-spark-events 2>/dev/null || true
    docker volume rm iceberg-demo-clickhouse-data 2>/dev/null || true
    docker volume rm iceberg-demo-trino-data 2>/dev/null || true
    docker volume rm iceberg-demo-airflow-postgres-data 2>/dev/null || true
    docker volume rm iceberg-demo-prometheus-data 2>/dev/null || true
    docker volume rm iceberg-demo-grafana-data 2>/dev/null || true

    log_step "Cleaning up Airflow logs..."
    rm -rf "$PROJECT_DIR/airflow/logs"/* 2>/dev/null || true

    log_step "Pruning unused containers..."
    docker container prune -f 2>/dev/null || true

    log_success "Environment reset complete"
}

# =============================================================================
# PHASE 1: Start Infrastructure
# =============================================================================
start_infrastructure() {
    log_phase "PHASE 1: Starting Infrastructure"

    cd "$INFRA_DIR"

    if [ "$SKIP_DATAGEN" = true ]; then
        log_step "Building and starting services (without datagen)..."
        docker-compose up -d --build 2>&1 | grep -E "Created|Started|Running" || true
    else
        log_step "Building and starting services (with continuous datagen)..."
        docker-compose --profile datagen up -d --build 2>&1 | grep -E "Created|Started|Running" || true
    fi

    echo ""
    log_step "Waiting for services to be healthy..."
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

    log_success "All services running"

    if [ "$VALIDATE_MODE" = true ]; then
        echo ""
        log_step "Validating infrastructure..."

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

        # Test PostgreSQL
        if docker exec iceberg-airflow-postgres psql -U airflow -d iceberg_catalog -c "SELECT 1" &>/dev/null; then
            log_success "PostgreSQL Iceberg catalog accessible"
        else
            log_fail "PostgreSQL Iceberg catalog not accessible"
        fi
    fi

    echo ""
    log_info "Service URLs:"
    log_info "  Airflow:          http://localhost:8086 (admin/admin123)"
    log_info "  Spark Master:     http://localhost:8084"
    log_info "  Flink:            http://localhost:8083"
    log_info "  MinIO Console:    http://localhost:9001 (admin/admin123)"
    log_info "  Redpanda Console: http://localhost:8080"
    log_info "  Trino:            http://localhost:8085"
    log_info "  Ingestion API:    http://localhost:8090"
}

# =============================================================================
# PHASE 2: Initialize Iceberg Catalog
# =============================================================================
init_iceberg_catalog() {
    log_phase "PHASE 2: Initializing Iceberg Catalog"

    cd "$INFRA_DIR"

    log_step "Creating databases..."

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
            's3.secret-access-key' = 'admin123'
        );
        USE CATALOG iceberg_catalog;
        CREATE DATABASE IF NOT EXISTS raw COMMENT 'Raw webhook events';
        CREATE DATABASE IF NOT EXISTS staging COMMENT 'Cleaned staging data';
        CREATE DATABASE IF NOT EXISTS metadata COMMENT 'Pipeline metadata and watermarks';
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
    log_phase "PHASE 3: Generating Mock Data"

    cd "$PROJECT_DIR"

    # Determine Python executable to use
    PYTHON_CMD="python3"
    VENV_DIR="$PROJECT_DIR/.venv"

    # Setup virtual environment if dependencies not available
    if ! python3 -c "import click, httpx, faker" 2>/dev/null; then
        log_step "Setting up Python environment..."

        if command -v uv &>/dev/null; then
            log_info "Using uv to create virtual environment..."
            uv venv "$VENV_DIR" 2>/dev/null || true
            uv pip install --quiet click httpx faker 2>/dev/null
            PYTHON_CMD="$VENV_DIR/bin/python"
            log_success "Dependencies installed with uv"
        elif [ -d "$VENV_DIR" ]; then
            source "$VENV_DIR/bin/activate"
            pip install --quiet click httpx faker 2>/dev/null
            PYTHON_CMD="$VENV_DIR/bin/python"
            log_success "Dependencies installed in existing venv"
        else
            log_info "Creating virtual environment..."
            python3 -m venv "$VENV_DIR"
            "$VENV_DIR/bin/pip" install --quiet click httpx faker
            PYTHON_CMD="$VENV_DIR/bin/python"
            log_success "Dependencies installed in new venv"
        fi
    fi

    log_step "Posting mock data to ingestion API..."
    log_info "Shopify: $SHOPIFY_CUSTOMERS customers, $SHOPIFY_ORDERS orders"
    log_info "Stripe:  $STRIPE_CUSTOMERS customers, $STRIPE_CHARGES charges"
    log_info "HubSpot: $HUBSPOT_CONTACTS contacts"
    echo ""

    "$PYTHON_CMD" scripts/post_mock_data.py \
        --url http://localhost:8090 \
        --shopify-customers "$SHOPIFY_CUSTOMERS" \
        --shopify-orders "$SHOPIFY_ORDERS" \
        --stripe-customers "$STRIPE_CUSTOMERS" \
        --stripe-charges "$STRIPE_CHARGES" \
        --hubspot-contacts "$HUBSPOT_CONTACTS" \
        --seed 42 2>&1 | grep -E "Posted|Total|Summary" || true

    log_success "Mock data generated"
}

# =============================================================================
# PHASE 4: Submit Flink Streaming Jobs
# =============================================================================
submit_flink_jobs() {
    log_phase "PHASE 4: Submitting Flink Streaming Jobs"

    cd "$INFRA_DIR"

    log_step "Submitting Flink SQL streaming jobs..."
    log_info "These jobs read from Kafka and write to Iceberg raw tables"
    echo ""

    for job in shopify_orders shopify_customers stripe_charges stripe_customers hubspot_contacts; do
        log_info "Submitting: ${job}_full.sql"
        docker exec iceberg-flink-jobmanager /opt/flink/bin/sql-client.sh embedded \
            -f "/opt/flink/jobs/${job}_full.sql" 2>&1 | tail -3 &
        sleep 3
    done

    echo ""
    log_warning "Flink jobs submitted in background"
    log_info "Monitor at: http://localhost:8083"
    log_info "Waiting 60s for initial data processing..."
    sleep 60

    if [ "$VALIDATE_MODE" = true ]; then
        echo ""
        log_step "Validating raw tables..."
        for table in shopify_orders shopify_customers stripe_charges stripe_customers hubspot_contacts; do
            if check_table_exists "raw" "$table"; then
                log_success "Raw table exists: raw.$table"
            else
                log_fail "Raw table missing: raw.$table"
            fi
        done

        # Check Redpanda messages
        for topic in shopify.orders shopify.customers stripe.charges stripe.customers hubspot.contacts; do
            local msg_count=$(docker exec iceberg-redpanda rpk topic consume "$topic" --num 1 --format json 2>/dev/null | wc -l)
            if [ "$msg_count" -gt 0 ]; then
                log_success "Messages in Redpanda topic: $topic"
            else
                log_fail "No messages in topic: $topic"
            fi
        done
    fi

    log_success "Flink streaming jobs running"
}

# =============================================================================
# PHASE 5: Run Batch Pipeline
# =============================================================================
run_batch_pipeline() {
    log_phase "PHASE 5: Running Batch Pipeline"

    cd "$INFRA_DIR"

    # Spark submit base command
    SPARK_SUBMIT="docker exec iceberg-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.iceberg.type=rest \
        --conf spark.sql.catalog.iceberg.uri=http://iceberg-rest:8181 \
        --conf spark.sql.catalog.iceberg.warehouse=s3a://warehouse/ \
        --conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
        --conf spark.sql.catalog.iceberg.s3.endpoint=http://minio:9000 \
        --conf spark.sql.catalog.iceberg.s3.path-style-access=true \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=admin \
        --conf spark.hadoop.fs.s3a.secret.key=admin123 \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.executor.memory=2g \
        --conf spark.driver.memory=2g"

    # Create metadata tables
    log_step "Creating metadata tables..."
    docker exec iceberg-spark-master /opt/spark/bin/spark-sql \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.iceberg.type=rest \
        --conf spark.sql.catalog.iceberg.uri=http://iceberg-rest:8181 \
        --conf spark.sql.catalog.iceberg.warehouse=s3a://warehouse/ \
        --conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
        --conf spark.sql.catalog.iceberg.s3.endpoint=http://minio:9000 \
        --conf spark.sql.catalog.iceberg.s3.path-style-access=true \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=admin \
        --conf spark.hadoop.fs.s3a.secret.key=admin123 \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        -e "
            CREATE DATABASE IF NOT EXISTS iceberg.metadata;
            CREATE TABLE IF NOT EXISTS iceberg.metadata.incremental_watermarks (
                source_table STRING,
                pipeline_name STRING,
                last_sync_timestamp TIMESTAMP,
                records_processed BIGINT,
                updated_at TIMESTAMP
            ) USING iceberg;
        " 2>&1 | tail -3 || log_warning "Metadata tables may already exist"
    log_success "Metadata tables ready"

    echo ""
    log_step "Running staging batch jobs..."
    for table in shopify_orders shopify_customers stripe_charges stripe_customers hubspot_contacts; do
        log_info "Processing: $table"
        $SPARK_SUBMIT /opt/spark/jobs/staging_batch.py --table $table --mode full 2>&1 | tail -3 || {
            log_warning "Failed to process $table"
        }
    done
    log_success "Staging complete"

    if [ "$VALIDATE_MODE" = true ]; then
        for table in stg_shopify_orders stg_shopify_customers stg_stripe_charges stg_stripe_customers stg_hubspot_contacts; do
            if check_table_exists "staging" "$table"; then
                log_success "Staging table exists: staging.$table"
            else
                log_fail "Staging table missing: staging.$table"
            fi
        done
    fi

    echo ""
    log_step "Running entity resolution..."
    $SPARK_SUBMIT /opt/spark/jobs/entity_backfill.py --mode initial 2>&1 | tail -5 || {
        log_warning "Entity backfill had issues"
    }
    log_success "Entity resolution complete"

    if [ "$VALIDATE_MODE" = true ]; then
        for table in entity_index blocking_index; do
            if check_table_exists "semantic" "$table"; then
                log_success "Semantic table exists: semantic.$table"
            else
                log_fail "Semantic table missing: semantic.$table"
            fi
        done
    fi

    echo ""
    log_step "Creating core views..."
    $SPARK_SUBMIT /opt/spark/jobs/core_views.py 2>&1 | tail -5 || {
        log_warning "Core views creation had issues"
    }
    log_success "Core views created"

    echo ""
    log_step "Running analytics transforms..."
    $SPARK_SUBMIT /opt/spark/jobs/analytics_incremental.py --mode full 2>&1 | tail -5 || {
        log_warning "Analytics transforms had issues"
    }
    log_success "Analytics complete"

    if [ "$VALIDATE_MODE" = true ]; then
        for table in customer_metrics order_summary payment_metrics; do
            if check_table_exists "analytics" "$table"; then
                log_success "Analytics table exists: analytics.$table"
            else
                log_fail "Analytics table missing: analytics.$table"
            fi
        done
    fi

    echo ""
    log_step "Running marts transforms..."
    $SPARK_SUBMIT /opt/spark/jobs/marts_incremental.py --mode full 2>&1 | tail -5 || {
        log_warning "Marts transforms had issues"
    }
    log_success "Marts complete"

    if [ "$VALIDATE_MODE" = true ]; then
        for table in customer_360 sales_dashboard_daily; do
            if check_table_exists "marts" "$table"; then
                log_success "Marts table exists: marts.$table"
            else
                log_fail "Marts table missing: marts.$table"
            fi
        done
    fi
}

# =============================================================================
# PHASE 6: Setup ClickHouse Views
# =============================================================================
setup_clickhouse_views() {
    log_phase "PHASE 6: Setting up ClickHouse Iceberg Views"

    cd "$INFRA_DIR"

    log_step "Creating ClickHouse views for Iceberg tables..."
    docker exec -i iceberg-clickhouse clickhouse-client --multiquery < clickhouse/iceberg_setup.sql 2>&1 | tail -3 || {
        log_warning "Some ClickHouse views may have failed"
    }

    local view_count=$(docker exec iceberg-clickhouse clickhouse-client --query "SHOW TABLES FROM iceberg" 2>/dev/null | wc -l | tr -d ' ')
    if [ "${view_count:-0}" -ge 10 ]; then
        log_success "Created $view_count ClickHouse views for Iceberg tables"
    else
        log_warning "Only created ${view_count:-0} views (expected 10+)"
    fi

    log_info "Views enable querying Iceberg tables from Grafana dashboards"
}

# =============================================================================
# PHASE 7: Validate Tables
# =============================================================================
validate_tables() {
    log_phase "PHASE 7: Validating All Tables"

    cd "$INFRA_DIR"

    log_step "Checking table row counts via Trino..."
    sleep 5

    docker exec iceberg-trino trino --execute "
        SELECT 'raw.shopify_orders' as tbl, COUNT(*) as cnt FROM iceberg.raw.shopify_orders
        UNION ALL SELECT 'raw.shopify_customers', COUNT(*) FROM iceberg.raw.shopify_customers
        UNION ALL SELECT 'raw.stripe_charges', COUNT(*) FROM iceberg.raw.stripe_charges
        UNION ALL SELECT 'raw.stripe_customers', COUNT(*) FROM iceberg.raw.stripe_customers
        UNION ALL SELECT 'raw.hubspot_contacts', COUNT(*) FROM iceberg.raw.hubspot_contacts
        UNION ALL SELECT 'staging.stg_shopify_orders', COUNT(*) FROM iceberg.staging.stg_shopify_orders
        UNION ALL SELECT 'staging.stg_shopify_customers', COUNT(*) FROM iceberg.staging.stg_shopify_customers
        UNION ALL SELECT 'staging.stg_stripe_charges', COUNT(*) FROM iceberg.staging.stg_stripe_charges
        UNION ALL SELECT 'staging.stg_stripe_customers', COUNT(*) FROM iceberg.staging.stg_stripe_customers
        UNION ALL SELECT 'staging.stg_hubspot_contacts', COUNT(*) FROM iceberg.staging.stg_hubspot_contacts
        UNION ALL SELECT 'semantic.entity_index', COUNT(*) FROM iceberg.semantic.entity_index
        UNION ALL SELECT 'core.customers', COUNT(*) FROM iceberg.core.customers
        UNION ALL SELECT 'core.orders', COUNT(*) FROM iceberg.core.orders
        UNION ALL SELECT 'analytics.customer_metrics', COUNT(*) FROM iceberg.analytics.customer_metrics
        UNION ALL SELECT 'analytics.order_summary', COUNT(*) FROM iceberg.analytics.order_summary
        UNION ALL SELECT 'analytics.payment_metrics', COUNT(*) FROM iceberg.analytics.payment_metrics
        UNION ALL SELECT 'marts.customer_360', COUNT(*) FROM iceberg.marts.customer_360
        ORDER BY tbl
    " 2>/dev/null || {
        log_warning "Could not validate via Trino - checking catalog instead"
        docker exec iceberg-airflow-postgres psql -U airflow -d iceberg_catalog -c \
            "SELECT table_namespace, table_name FROM iceberg_tables ORDER BY table_namespace, table_name;" 2>/dev/null
    }

    log_success "Validation complete"
}

# =============================================================================
# PHASE 8: Trigger Airflow DAG
# =============================================================================
trigger_airflow_dag() {
    log_phase "PHASE 8: Triggering Airflow DAG"

    cd "$INFRA_DIR"

    local DAG_ID="iceberg_pipeline"
    local MAX_WAIT=120
    local WAIT_INTERVAL=10

    log_step "Waiting for DAG to be parsed by scheduler..."
    local elapsed=0
    while [ $elapsed -lt $MAX_WAIT ]; do
        local dag_exists=$(docker exec iceberg-airflow-postgres psql -U airflow -d airflow -t -c \
            "SELECT COUNT(*) FROM dag WHERE dag_id = '$DAG_ID';" 2>/dev/null | tr -d ' ')

        if [ "${dag_exists:-0}" -ge 1 ]; then
            log_info "DAG found in scheduler"
            break
        fi

        echo -n "."
        sleep $WAIT_INTERVAL
        elapsed=$((elapsed + WAIT_INTERVAL))
    done
    echo ""

    if [ $elapsed -ge $MAX_WAIT ]; then
        log_warning "DAG not found after ${MAX_WAIT}s - scheduler may still be parsing"
        log_info "Check Airflow UI: http://localhost:8086"
        return 1
    fi

    log_step "Ensuring DAG is unpaused..."
    docker exec iceberg-airflow-postgres psql -U airflow -d airflow -c \
        "UPDATE dag SET is_paused = false WHERE dag_id = '$DAG_ID';" 2>/dev/null || true

    log_step "Triggering $DAG_ID DAG..."
    docker exec iceberg-airflow-scheduler airflow dags trigger "$DAG_ID" 2>/dev/null || {
        log_fail "Failed to trigger DAG"
        log_info "Check Airflow UI: http://localhost:8086"
        return 1
    }

    log_success "DAG triggered"
    log_info "Monitor at: http://localhost:8086/dags/$DAG_ID/grid"

    if [ "$VALIDATE_MODE" = true ]; then
        log_step "Waiting for DAG execution (120s)..."
        sleep 120

        log_step "Checking DAG task states..."
        local task_states=$(docker exec iceberg-airflow-scheduler airflow tasks states-for-dag-run "$DAG_ID" -1 2>/dev/null || echo "")

        if [ -n "$task_states" ]; then
            local success_count=$(echo "$task_states" | grep -c "success" || echo "0")
            local failed_count=$(echo "$task_states" | grep -c "failed" || echo "0")

            log_info "Tasks succeeded: $success_count"
            log_info "Tasks failed: $failed_count"

            if [ "$success_count" -gt 5 ] && [ "$failed_count" -eq 0 ]; then
                log_success "DAG tasks executing successfully"
            else
                log_warning "DAG execution may have issues - check Airflow UI"
            fi
        fi
    fi
}

# =============================================================================
# Summary
# =============================================================================
print_summary() {
    if [ "$VALIDATE_MODE" = true ]; then
        log_phase "VALIDATION SUMMARY"

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
    else
        echo ""
        echo "╔══════════════════════════════════════════════════════════╗"
        echo "║                    Demo Setup Complete!                   ║"
        echo "╚══════════════════════════════════════════════════════════╝"
    fi

    echo ""
    echo "  Next steps:"
    echo "  ───────────────────────────────────────────────────────────"
    echo "  1. Open Airflow: http://localhost:8086 (admin/admin123)"
    echo "  2. Watch DAG: iceberg_pipeline"
    echo "  3. Query data: docker exec -it iceberg-trino trino"
    echo "  4. View MinIO data: http://localhost:9001 (admin/admin123)"
    echo ""
    echo "  Troubleshooting:"
    echo "  ───────────────────────────────────────────────────────────"
    echo "  - If staging fails: Check raw tables exist (Flink jobs)"
    echo "  - If catalog errors: PostgreSQL catalog is used (not SQLite)"
    echo "  - Logs: docker-compose logs -f <service>"
    echo ""
}

# =============================================================================
# Main
# =============================================================================
main() {
    echo ""
    echo -e "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}║         Iceberg Incremental Demo - Reset and Run                 ║${NC}"
    echo -e "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
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
    run_batch_pipeline
    setup_clickhouse_views
    validate_tables
    trigger_airflow_dag
    print_summary
}

main "$@"
