#!/bin/bash
# =============================================================================
# Initialize PostgreSQL databases for Iceberg REST Catalog
# =============================================================================
# This script runs on first PostgreSQL start to create the iceberg database.
# PostgreSQL's docker image runs scripts in /docker-entrypoint-initdb.d/
# =============================================================================

set -e

# Create database for Iceberg REST catalog
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create iceberg database for the REST catalog
    CREATE DATABASE iceberg_catalog;

    -- Grant privileges (optional, since we use the same user)
    GRANT ALL PRIVILEGES ON DATABASE iceberg_catalog TO $POSTGRES_USER;

    \c iceberg_catalog

    -- Create schema for iceberg metadata (optional, catalog creates its own tables)
    CREATE SCHEMA IF NOT EXISTS iceberg;

    -- Log success
    DO \$\$
    BEGIN
        RAISE NOTICE 'Iceberg catalog database initialized successfully';
    END
    \$\$;
EOSQL

echo "Iceberg catalog database 'iceberg_catalog' created successfully"
