#!/bin/bash
# =============================================================================
# Validate Iceberg Tables
# =============================================================================
# Quick validation script to check all tables exist and have data.
#
# Usage:
#   ./scripts/validate_tables.sh
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
INFRA_DIR="$PROJECT_DIR/infrastructure"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

cd "$INFRA_DIR"

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║           Iceberg Tables Validation                       ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

echo -e "${BLUE}Checking table row counts via Trino...${NC}"
echo ""

# Core pipeline tables (excluding optional metadata tables)
docker exec iceberg-trino trino --execute "
SELECT 'raw.shopify_orders' as table_name, COUNT(*) as row_count FROM iceberg.raw.shopify_orders
UNION ALL SELECT 'raw.shopify_customers', COUNT(*) FROM iceberg.raw.shopify_customers
UNION ALL SELECT 'raw.stripe_charges', COUNT(*) FROM iceberg.raw.stripe_charges
UNION ALL SELECT 'raw.stripe_customers', COUNT(*) FROM iceberg.raw.stripe_customers
UNION ALL SELECT 'raw.hubspot_contacts', COUNT(*) FROM iceberg.raw.hubspot_contacts
UNION ALL SELECT 'staging.stg_shopify_orders', COUNT(*) FROM iceberg.staging.stg_shopify_orders
UNION ALL SELECT 'staging.stg_shopify_customers', COUNT(*) FROM iceberg.staging.stg_shopify_customers
UNION ALL SELECT 'staging.stg_stripe_charges', COUNT(*) FROM iceberg.staging.stg_stripe_charges
UNION ALL SELECT 'staging.stg_hubspot_contacts', COUNT(*) FROM iceberg.staging.stg_hubspot_contacts
UNION ALL SELECT 'semantic.entity_index', COUNT(*) FROM iceberg.semantic.entity_index
UNION ALL SELECT 'semantic.blocking_index', COUNT(*) FROM iceberg.semantic.blocking_index
UNION ALL SELECT 'core.customers', COUNT(*) FROM iceberg.core.customers
UNION ALL SELECT 'core.orders', COUNT(*) FROM iceberg.core.orders
UNION ALL SELECT 'analytics.customer_metrics', COUNT(*) FROM iceberg.analytics.customer_metrics
UNION ALL SELECT 'analytics.order_summary', COUNT(*) FROM iceberg.analytics.order_summary
UNION ALL SELECT 'analytics.payment_metrics', COUNT(*) FROM iceberg.analytics.payment_metrics
UNION ALL SELECT 'marts.customer_360', COUNT(*) FROM iceberg.marts.customer_360
ORDER BY table_name
"

echo ""
echo -e "${BLUE}Validation complete.${NC}"
echo ""
