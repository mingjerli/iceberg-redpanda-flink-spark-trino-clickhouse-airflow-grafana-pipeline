-- =============================================================================
-- Flink SQL: Staging Layer Setup
-- =============================================================================
-- This script sets up the Iceberg catalog and staging database for the
-- staging layer transforms.
--
-- Run this once before starting the staging jobs.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Create Iceberg Catalog Connection
-- -----------------------------------------------------------------------------
CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181',
    'warehouse' = 's3a://warehouse/',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://minio:9000',
    's3.path-style-access' = 'true'
);

-- Use the Iceberg catalog by default
USE CATALOG iceberg_catalog;

-- -----------------------------------------------------------------------------
-- Create Staging Database
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS staging
COMMENT 'Staging layer - cleaned and typed data from raw sources';

-- Verify raw database exists (dependency)
CREATE DATABASE IF NOT EXISTS `raw`
COMMENT 'Raw layer - append-only webhook events';
