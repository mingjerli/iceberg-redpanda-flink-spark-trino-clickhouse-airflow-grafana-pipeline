#!/usr/bin/env bash
# =============================================================================
# Run the test suite
# =============================================================================
# Tests run inside the project's Spark image, which already carries Java 11,
# Spark 3.5.3, and the Iceberg 1.5.0 runtime jars -- the same versions the
# pipeline runs in production. Running them on the host would need a JDK plus a
# matching local PySpark.
#
# No other service is required: the tests use a hadoop-type Iceberg catalog in a
# temp directory, so MinIO, Postgres, and the REST catalog can all be down.
#
# Usage:
#   ./scripts/run_tests.sh                          # whole suite
#   ./scripts/run_tests.sh tests/test_ga4_dedup.py  # one file
#   ./scripts/run_tests.sh -k dedup -vv             # pytest args pass through
# =============================================================================
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Built by `docker-compose build spark-master` from infrastructure/spark/Dockerfile.
SPARK_IMAGE="${SPARK_TEST_IMAGE:-infrastructure-spark-master}"

# py4j ships inside the Spark distribution; keep in step with SPARK_VERSION.
PY4J_ZIP="/opt/spark/python/lib/py4j-0.10.9.7-src.zip"

if ! docker image inspect "$SPARK_IMAGE" >/dev/null 2>&1; then
    echo -e "${YELLOW}Image '$SPARK_IMAGE' not found. Building it...${NC}"
    (cd "$PROJECT_DIR/infrastructure" && docker-compose build spark-master) || {
        echo -e "${RED}Could not build the Spark image.${NC}"
        echo "Set SPARK_TEST_IMAGE to an existing image if yours is named differently."
        exit 1
    }
fi

echo -e "${GREEN}Running tests in $SPARK_IMAGE${NC}"

# --user root so pip can install the test dependencies into the container.
docker run --rm --user root \
    -v "$PROJECT_DIR":/work -w /work \
    --entrypoint bash "$SPARK_IMAGE" -c "
        set -e
        pip3 install --quiet --disable-pip-version-check -r requirements-dev.txt
        export PYTHONPATH=/opt/spark/python:$PY4J_ZIP:/work
        export SPARK_LOCAL_IP=127.0.0.1
        python3 -m pytest ${*:-tests/} -q
    "

echo -e "${GREEN}Tests passed${NC}"
