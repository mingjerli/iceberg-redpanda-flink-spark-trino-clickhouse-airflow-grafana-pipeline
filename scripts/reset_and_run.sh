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
#   MAILCHIMP_SUBSCRIBERS Number of Mailchimp subscribers (default: 100)
#   MAILCHIMP_CAMPAIGNS   Number of Mailchimp campaigns (default: 20)
#   MAILCHIMP_EVENTS      Number of Mailchimp events (default: 500)
# =============================================================================

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
INFRA_DIR="$PROJECT_DIR/infrastructure"
ENV_FILE="$INFRA_DIR/.env"
ENV_EXAMPLE="$INFRA_DIR/.env.example"

# Check for .env file and copy from .env.example if it doesn't exist
if [ ! -f "$ENV_FILE" ]; then
    if [ -f "$ENV_EXAMPLE" ]; then
        echo "Creating .env file from .env.example..."
        cp "$ENV_EXAMPLE" "$ENV_FILE"
        echo "Created $ENV_FILE - please review and customize if needed."
    else
        echo "ERROR: Neither $ENV_FILE nor $ENV_EXAMPLE found!"
        echo "Please create $ENV_FILE with required configuration."
        exit 1
    fi
fi

# Source environment variables from .env file
set -a  # automatically export all variables
source "$ENV_FILE"
set +a

# jobs/spark/staging_batch.py reads PII_TOKEN_PEPPER via os.environ.get at
# import time and every staging function calls tokenize_frame(), which
# raises immediately if the pepper is empty -- even for tables with no
# registered PII columns. Fail here, before starting any infrastructure,
# rather than 5+ minutes into Phase 5 on the first staging job's stack trace.
# `set -a` above already exports PII_TOKEN_PEPPER into this script's own
# environment; docker-compose.yml separately declares it on spark-master's
# environment (read from this same $ENV_FILE), which is what actually
# reaches spark-submit, since `docker exec` inherits the target container's
# environment, not the caller's.
if [ -z "${PII_TOKEN_PEPPER:-}" ]; then
    echo "ERROR: PII_TOKEN_PEPPER is not set in $ENV_FILE."
    echo "  Generate one with: openssl rand -hex 32"
    echo "  Then set PII_TOKEN_PEPPER=<value> in $ENV_FILE and re-run."
    exit 1
elif [ "$PII_TOKEN_PEPPER" = "change-me-generate-with-openssl-rand-hex-32" ]; then
    echo "WARNING: PII_TOKEN_PEPPER in $ENV_FILE is still the placeholder from .env.example."
    echo "  The demo will run, but every token is derived from a publicly known pepper."
    echo "  Generate a real one with: openssl rand -hex 32"
fi

# Data generation settings
SHOPIFY_CUSTOMERS=${SHOPIFY_CUSTOMERS:-50}
SHOPIFY_ORDERS=${SHOPIFY_ORDERS:-100}
STRIPE_CUSTOMERS=${STRIPE_CUSTOMERS:-30}
STRIPE_CHARGES=${STRIPE_CHARGES:-80}
HUBSPOT_CONTACTS=${HUBSPOT_CONTACTS:-40}
MAILCHIMP_SUBSCRIBERS=${MAILCHIMP_SUBSCRIBERS:-100}
MAILCHIMP_CAMPAIGNS=${MAILCHIMP_CAMPAIGNS:-20}
MAILCHIMP_EVENTS=${MAILCHIMP_EVENTS:-500}

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
            echo "  MAILCHIMP_SUBSCRIBERS Number of Mailchimp subscribers (default: 100)"
            echo "  MAILCHIMP_CAMPAIGNS   Number of Mailchimp campaigns (default: 20)"
            echo "  MAILCHIMP_EVENTS      Number of Mailchimp events (default: 500)"
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

wait_for_raw_tables() {
    # Flink writes the raw Iceberg tables on its first checkpoint commit, which
    # is well after the jobs report as running and well after messages land in
    # Redpanda. On a from-scratch run there is no pre-existing table, so the
    # batch phase must wait for the commit rather than a fixed span.
    local budget=${1:-180}
    shift
    local deadline=$(( SECONDS + budget ))
    local missing table

    echo -n "  Waiting for Flink to commit raw tables..."
    while [ "$SECONDS" -lt "$deadline" ]; do
        missing=""
        for table in "$@"; do
            check_table_exists "raw" "$table" || missing="$missing $table"
        done

        if [ -z "$missing" ]; then
            echo -e " ${GREEN}ready${NC}"
            return 0
        fi

        echo -n "."
        sleep 3
    done

    echo -e " ${RED}timeout${NC}"
    log_fail "Raw tables never committed after ${budget}s:${missing}"
    log_info "Flink creates these on its first checkpoint. Check http://localhost:8083"
    return 1
}

check_table_exists() {
    local namespace=$1
    local table=$2
    local count=$(docker exec iceberg-airflow-postgres psql -U airflow -d iceberg_catalog -t -c \
        "SELECT COUNT(*) FROM iceberg_tables WHERE table_namespace = '$namespace' AND table_name = '$table';" 2>/dev/null | tr -d ' ')
    [ "${count:-0}" -ge 1 ]
}

# Run a Spark job and report its real outcome.
#
# `cmd 2>&1 | tail -N` yields tail's exit status, never the job's, so the
# `|| log_warning` guards that used to wrap these calls could not fire and a
# hard failure was still announced with log_success. That is how a broken
# entity-resolution run and an ingest reading a nonexistent path both reported
# themselves complete while leaving every downstream layer empty.
run_spark_job() {
    local description=$1
    shift

    local job_log status=0
    job_log=$(mktemp)

    # `|| status=$?` keeps `set -e` from aborting before we can report.
    $SPARK_SUBMIT "$@" > "$job_log" 2>&1 || status=$?

    tail -3 "$job_log"

    if [ "$status" -ne 0 ]; then
        log_fail "$description failed (exit $status)"

        # The JVM stack trails the message that names the cause, so `tail`
        # alone buries it -- that is why the original incident took several
        # rounds to diagnose. Surface the exception lines first, then keep the
        # whole log instead of deleting the only copy.
        local exception
        exception=$(grep -aE "^(Traceback|[A-Za-z_.]+(Error|Exception)[:( ])|Caused by:" "$job_log" | tail -10)
        if [ -n "$exception" ]; then
            echo "  ---- exception ----"
            echo "$exception" | sed 's/^/  /'
        fi

        echo "  ---- last 25 lines ----"
        tail -25 "$job_log" | sed 's/^/  /'

        local kept="${SPARK_JOB_LOG_DIR:-/tmp}/spark-job-$(echo "$description" | tr -cs '[:alnum:]' '-').log"
        if cp "$job_log" "$kept" 2>/dev/null; then
            echo "  full log: $kept"
        fi
        rm -f "$job_log"
        return 1
    fi

    rm -f "$job_log"
    return 0
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

    # Substitute credential placeholders in config templates
    log_step "Generating config files from templates..."
    sed -e "s/__MINIO_USER__/$MINIO_ROOT_USER/g" \
        -e "s/__MINIO_PASSWORD__/$MINIO_ROOT_PASSWORD/g" \
        trino/catalog/iceberg.properties > /tmp/trino_iceberg.properties
    cp /tmp/trino_iceberg.properties trino/catalog/iceberg.properties
    rm -f /tmp/trino_iceberg.properties

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
    docker exec iceberg-minio mc alias set myminio http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" 2>/dev/null || true
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
    log_info "  Homepage:         http://localhost:${EXTERNAL_HOMEPAGE_PORT:-8087}  <- links to everything below"
    log_info "  Airflow:          http://localhost:8086 ($AIRFLOW_ADMIN_USER/$AIRFLOW_ADMIN_PASSWORD)"
    log_info "  Grafana:          http://localhost:${EXTERNAL_GRAFANA_PORT:-3000} (${GRAFANA_ADMIN_USER:-admin}/${GRAFANA_ADMIN_PASSWORD:-admin123})"
    log_info "  Spark Master:     http://localhost:8084"
    log_info "  Flink:            http://localhost:8083"
    log_info "  MinIO Console:    http://localhost:9001 ($MINIO_ROOT_USER/$MINIO_ROOT_PASSWORD)"
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
            's3.access-key-id' = '$MINIO_ROOT_USER',
            's3.secret-access-key' = '$MINIO_ROOT_PASSWORD'
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

    # Install from the declared requirements files rather than a hand-kept list.
    # That list had drifted from what the code actually imports -- it lacked
    # pandas/pyarrow (the GA4 Parquet writer) and orjson, so GA4 generation died
    # on whichever was missing first. The two files together cover click,
    # orjson, tqdm, pandas, pyarrow, httpx and faker.
    #
    # Install errors are no longer sent to /dev/null: a silent failure here
    # surfaces much later as a confusing ModuleNotFoundError.
    local req_datagen="$PROJECT_DIR/datagen/requirements.txt"
    local req_scripts="$PROJECT_DIR/scripts/requirements.txt"

    if ! python3 -c "import click, httpx, faker, orjson, pandas, pyarrow" 2>/dev/null; then
        log_step "Setting up Python environment..."

        if command -v uv &>/dev/null; then
            log_info "Using uv to create virtual environment..."
            uv venv "$VENV_DIR" 2>/dev/null || true
            uv pip install --quiet --python "$VENV_DIR/bin/python" \
                -r "$req_datagen" -r "$req_scripts"
            PYTHON_CMD="$VENV_DIR/bin/python"
            log_success "Dependencies installed with uv"
        else
            [ -d "$VENV_DIR" ] || { log_info "Creating virtual environment..."; python3 -m venv "$VENV_DIR"; }
            "$VENV_DIR/bin/pip" install --quiet -r "$req_datagen" -r "$req_scripts"
            PYTHON_CMD="$VENV_DIR/bin/python"
            log_success "Dependencies installed in venv"
        fi
    fi

    log_step "Posting mock data to ingestion API..."
    log_info "Shopify: $SHOPIFY_CUSTOMERS customers, $SHOPIFY_ORDERS orders"
    log_info "Stripe:  $STRIPE_CUSTOMERS customers, $STRIPE_CHARGES charges"
    log_info "HubSpot: $HUBSPOT_CONTACTS contacts"
    log_info "Mailchimp: $MAILCHIMP_SUBSCRIBERS subscribers, $MAILCHIMP_CAMPAIGNS campaigns, $MAILCHIMP_EVENTS events"
    log_info "GA4: one Parquet export (generator default: 200 users, 3-20 events each)"
    echo ""

    # Keep the full output so the failure count survives the grep below.
    local post_log
    post_log=$(mktemp)

    "$PYTHON_CMD" scripts/post_mock_data.py \
        --url http://localhost:8090 \
        --shopify-customers "$SHOPIFY_CUSTOMERS" \
        --shopify-orders "$SHOPIFY_ORDERS" \
        --stripe-customers "$STRIPE_CUSTOMERS" \
        --stripe-charges "$STRIPE_CHARGES" \
        --hubspot-contacts "$HUBSPOT_CONTACTS" \
        --mailchimp-subscribers "$MAILCHIMP_SUBSCRIBERS" \
        --mailchimp-campaigns "$MAILCHIMP_CAMPAIGNS" \
        --mailchimp-events "$MAILCHIMP_EVENTS" \
        --seed 42 2>&1 | tee "$post_log" | grep -E "Posted|Total|Summary" || true

    # post_mock_data.py reports its own failures, but piping into grep replaces
    # its exit status with grep's and `|| true` discards even that. A run where
    # every POST returned 500 therefore looked identical to a clean one, and the
    # script went on to submit Flink jobs against topics that were empty.
    local failed_posts
    failed_posts=$(grep -oE "Total Failed: [0-9]+" "$post_log" | grep -oE "[0-9]+$" | tail -1)
    rm -f "$post_log"

    if [ "${failed_posts:-0}" -gt 0 ]; then
        log_fail "Ingestion API rejected $failed_posts webhook posts -- nothing reached Redpanda"
        log_info "Inspect with: docker logs iceberg-ingestion-api --tail 50"
        exit 1
    fi

    # GA4 is the one source that is not a webhook: it arrives as a Parquet
    # export, standing in for a BigQuery Export. Without this step nothing ever
    # writes datagen/output/ga4/, so ga4_batch_ingest.py reads a path that does
    # not exist and every GA4 table downstream stays empty.
    #
    # The generator is driven from datagen/ because it imports its providers by
    # relative path, and writes ga4/events.parquet -- which docker-compose
    # mounts read-only at $GA4_EXPORT_PATH inside the Spark containers.
    echo ""
    log_step "Generating GA4 Parquet export..."
    (
        cd "$PROJECT_DIR/datagen" && \
        "$PYTHON_CMD" generator.py \
            --source ga4 \
            --output-dir ./output \
            --seed 42 2>&1 | grep -E "Saved|events|Error|error" || true
    )

    local ga4_export="$PROJECT_DIR/datagen/output/ga4/events.parquet"
    if [ ! -f "$ga4_export" ]; then
        log_fail "GA4 export not written to $ga4_export"
        log_info "Check the generator output above; deps come from datagen/requirements.txt"
        exit 1
    fi
    log_success "GA4 export written: $(basename "$ga4_export")"

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

    for job in shopify_orders shopify_customers stripe_charges stripe_customers hubspot_contacts mailchimp_campaigns mailchimp_events mailchimp_subscribers; do
        log_info "Submitting: ${job}_full.sql"
        docker exec iceberg-flink-jobmanager /opt/flink/bin/sql-client.sh embedded \
            -f "/opt/flink/jobs/${job}_full.sql" 2>&1 | tail -3 &
        sleep 3
    done

    echo ""
    log_warning "Flink jobs submitted in background"
    log_info "Monitor at: http://localhost:8083"
    log_info "Waiting for Flink to commit the raw tables..."
    wait_for_raw_tables "${RAW_TABLE_WAIT_SECONDS:-300}" \
        shopify_orders shopify_customers stripe_charges stripe_customers \
        hubspot_contacts mailchimp_campaigns mailchimp_events mailchimp_subscribers \
        || return 1

    if [ "$VALIDATE_MODE" = true ]; then
        echo ""
        log_step "Validating raw tables..."
        for table in shopify_orders shopify_customers stripe_charges stripe_customers hubspot_contacts mailchimp_campaigns mailchimp_events mailchimp_subscribers; do
            if check_table_exists "raw" "$table"; then
                log_success "Raw table exists: raw.$table"
            else
                log_fail "Raw table missing: raw.$table"
            fi
        done

        # Check Redpanda messages.
        #
        # Sum the per-partition high watermarks rather than consuming.
        # `rpk topic consume --num 1` blocks until a message arrives, so an
        # empty topic hangs this loop forever instead of reporting the very
        # failure it exists to catch -- and `timeout` is not available on macOS.
        # `describe` is a metadata read: it returns immediately, and 0 is a
        # perfectly good answer.
        for topic in shopify.orders shopify.customers stripe.charges stripe.customers hubspot.contacts mailchimp.campaigns mailchimp.events mailchimp.subscribers; do
            local msg_count
            msg_count=$(docker exec iceberg-redpanda rpk topic describe "$topic" -p 2>/dev/null \
                | awk 'NR>1 && $NF ~ /^[0-9]+$/ {sum += $NF} END {print sum+0}')
            if [ "${msg_count:-0}" -gt 0 ]; then
                log_success "Messages in Redpanda topic: $topic ($msg_count)"
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
        --conf spark.hadoop.fs.s3a.access.key=$MINIO_ROOT_USER \
        --conf spark.hadoop.fs.s3a.secret.key=$MINIO_ROOT_PASSWORD \
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
        --conf spark.hadoop.fs.s3a.access.key=$MINIO_ROOT_USER \
        --conf spark.hadoop.fs.s3a.secret.key=$MINIO_ROOT_PASSWORD \
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
    log_step "Running GA4 batch ingest..."
    log_info "Ingesting GA4 Parquet exports to raw layer"
    # --input is required; --mode accepts append|overwrite only. MERGE INTO on
    # _raw_id keeps re-runs idempotent, so append is safe on a reset.
    if run_spark_job "GA4 batch ingest" /opt/spark/jobs/ga4_batch_ingest.py \
        --input "${GA4_EXPORT_PATH:-/opt/spark/data/ga4/events.parquet}" \
        --mode append; then
        log_success "GA4 batch ingest complete"
    fi

    echo ""
    log_step "Running staging batch jobs..."
    # These are STAGING_FUNCTIONS keys, not table names -- no stg_ prefix.
    # `--table stg_ga4_events` is rejected by argparse before Spark starts.
    for table in shopify_orders shopify_customers stripe_charges stripe_customers hubspot_contacts mailchimp_campaigns mailchimp_events mailchimp_subscribers ga4_events ga4_sessions; do
        log_info "Processing: $table"
        run_spark_job "Staging $table" /opt/spark/jobs/staging_batch.py \
            --table "$table" --mode full || staging_failed=1
    done
    if [ "${staging_failed:-0}" -eq 0 ]; then
        log_success "Staging complete"
    else
        log_fail "Staging finished with failures -- downstream layers will be incomplete"
    fi

    if [ "$VALIDATE_MODE" = true ]; then
        for table in stg_shopify_orders stg_shopify_customers stg_stripe_charges stg_stripe_customers stg_hubspot_contacts stg_mailchimp_campaigns stg_mailchimp_events stg_mailchimp_subscribers stg_ga4_events stg_ga4_sessions; do
            if check_table_exists "staging" "$table"; then
                log_success "Staging table exists: staging.$table"
            else
                log_fail "Staging table missing: staging.$table"
            fi
        done
    fi

    echo ""
    log_step "Running entity resolution..."
    if run_spark_job "Entity resolution" /opt/spark/jobs/entity_backfill.py --mode initial; then
        log_success "Entity resolution complete"
    fi

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
    if run_spark_job "Core views" /opt/spark/jobs/core_views.py; then
        log_success "Core views created"
    fi

    echo ""
    log_step "Running analytics transforms..."
    if run_spark_job "Analytics transforms" /opt/spark/jobs/analytics_incremental.py --mode full; then
        log_success "Analytics complete"
    fi

    if [ "$VALIDATE_MODE" = true ]; then
        for table in customer_metrics order_summary payment_metrics campaign_metrics ga4_engagement_metrics ga4_engagement_by_channel ga4_page_performance ga4_funnel_analysis; do
            if check_table_exists "analytics" "$table"; then
                log_success "Analytics table exists: analytics.$table"
            else
                log_fail "Analytics table missing: analytics.$table"
            fi
        done
    fi

    echo ""
    log_step "Running marts transforms..."
    if run_spark_job "Marts transforms" /opt/spark/jobs/marts_incremental.py --mode full; then
        log_success "Marts complete"
    fi

    if [ "$VALIDATE_MODE" = true ]; then
        for table in customer_360 sales_dashboard_daily campaign_dashboard ga4_engagement_dashboard; do
            if check_table_exists "marts" "$table"; then
                log_success "Marts table exists: marts.$table"
            else
                log_fail "Marts table missing: marts.$table"
            fi
        done
    fi

    # Publish row, file, and snapshot counts for every layer. Runs last so the
    # numbers describe the finished state. Read-only, and the job swallows its
    # own errors, so this never fails the run.
    echo ""
    log_step "Publishing pipeline metrics..."
    if run_spark_job "Table metrics export" /opt/spark/jobs/export_metrics.py; then
        log_success "Metrics published"
    fi
}

# =============================================================================
# PHASE 6: Setup ClickHouse Views
# =============================================================================
setup_clickhouse_views() {
    log_phase "PHASE 6: Setting up ClickHouse Iceberg Views"

    cd "$INFRA_DIR"

    log_step "Creating ClickHouse views for Iceberg tables..."
    # Substitute environment variables in the SQL template
    sed -e "s/__MINIO_USER__/$MINIO_ROOT_USER/g" \
        -e "s/__MINIO_PASSWORD__/$MINIO_ROOT_PASSWORD/g" \
        clickhouse/iceberg_setup.sql | \
        docker exec -i iceberg-clickhouse clickhouse-client --multiquery 2>&1 | tail -3 || {
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

    # Define all tables to validate
    local tables=(
        "raw.shopify_orders"
        "raw.shopify_customers"
        "raw.stripe_charges"
        "raw.stripe_customers"
        "raw.hubspot_contacts"
        "raw.mailchimp_campaigns"
        "raw.mailchimp_events"
        "raw.mailchimp_subscribers"
        "raw.ga4_events"
        "staging.stg_shopify_orders"
        "staging.stg_shopify_customers"
        "staging.stg_stripe_charges"
        "staging.stg_stripe_customers"
        "staging.stg_hubspot_contacts"
        "staging.stg_mailchimp_campaigns"
        "staging.stg_mailchimp_events"
        "staging.stg_mailchimp_subscribers"
        "staging.stg_ga4_events"
        "staging.stg_ga4_sessions"
        "semantic.entity_index"
        "semantic.blocking_index"
        "core.customers"
        "core.orders"
        "analytics.customer_metrics"
        "analytics.order_summary"
        "analytics.payment_metrics"
        "analytics.campaign_metrics"
        "analytics.ga4_engagement_metrics"
        "analytics.ga4_engagement_by_channel"
        "analytics.ga4_page_performance"
        "analytics.ga4_funnel_analysis"
        "marts.customer_360"
        "marts.sales_dashboard_daily"
        "marts.executive_summary"
        "marts.campaign_dashboard"
        "marts.ga4_engagement_dashboard"
    )

    local all_passed=true
    local total_tables=${#tables[@]}
    local validated=0
    local failed=0

    printf "\n  %-40s %s\n" "TABLE" "ROW COUNT"
    printf "  %-40s %s\n" "----------------------------------------" "----------"

    for table in "${tables[@]}"; do
        local count
        count=$(docker exec iceberg-trino trino --execute \
            "SELECT COUNT(*) FROM iceberg.${table}" 2>/dev/null | tr -d '"')

        if [ $? -eq 0 ] && [ -n "$count" ]; then
            if [ "$count" -gt 0 ] 2>/dev/null; then
                printf "  %-40s %s\n" "$table" "$count ✓"
                validated=$((validated + 1))
            else
                printf "  %-40s %s\n" "$table" "0 (empty)"
                validated=$((validated + 1))
            fi
        else
            printf "  %-40s %s\n" "$table" "FAILED ✗"
            failed=$((failed + 1))
            all_passed=false
        fi
    done

    echo ""
    log_info "Validated: $validated/$total_tables tables"
    if [ $failed -gt 0 ]; then
        log_warning "Failed to query $failed table(s)"
    fi

    if [ "$all_passed" = true ]; then
        log_success "All tables validated via Trino"
    else
        log_warning "Some tables could not be validated"
    fi
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
    echo "  1. Open the homepage: http://localhost:${EXTERNAL_HOMEPAGE_PORT:-8087}"
    echo "     Every component UI, grouped by pipeline stage"
    echo "  2. Open Airflow: http://localhost:8086 ($AIRFLOW_ADMIN_USER/$AIRFLOW_ADMIN_PASSWORD)"
    echo "  3. Watch DAG: iceberg_pipeline"
    echo "  4. Query data: docker exec -it iceberg-trino trino"
    echo "  5. View MinIO data: http://localhost:9001 ($MINIO_ROOT_USER/$MINIO_ROOT_PASSWORD)"
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

# Only run when executed, not when sourced. Sourcing used to run main(), whose
# first act is removing every volume -- so `source reset_and_run.sh` to reach a
# helper destroyed the environment. It also makes the helpers testable.
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    main "$@"
fi
