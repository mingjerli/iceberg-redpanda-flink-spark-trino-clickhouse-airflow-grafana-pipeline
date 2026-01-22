-- =============================================================================
-- Flink SQL: Raw Ingestion Setup
-- =============================================================================
-- This script sets up the Flink catalogs and connections required for
-- streaming data from Redpanda to Iceberg tables.
--
-- Run this once before starting the ingestion jobs.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Create Iceberg Catalog Connection
-- -----------------------------------------------------------------------------
CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181',
    'warehouse' = 's3://warehouse/',
    's3.endpoint' = 'http://minio:9000',
    's3.path-style-access' = 'true',
    's3.access-key-id' = 'admin',
    's3.secret-access-key' = 'admin123456'
);

-- Use the Iceberg catalog by default
USE CATALOG iceberg_catalog;

-- -----------------------------------------------------------------------------
-- Create Raw Schema/Database
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS `raw`
COMMENT 'Raw layer - append-only webhook events';

-- Create Metadata Schema/Database
CREATE DATABASE IF NOT EXISTS metadata
COMMENT 'Metadata tables for tracking and watermarks';
