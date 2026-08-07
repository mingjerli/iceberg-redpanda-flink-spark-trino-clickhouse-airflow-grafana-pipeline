-- =============================================================================
-- Analytics: ga4_funnel_analysis
-- =============================================================================
-- Step-by-step conversion funnel, computed from staging.stg_ga4_events.
-- Grain: one row per (metric_date, funnel_name, step_number). The e-commerce
-- funnel runs page_view -> view_item -> add_to_cart -> begin_checkout -> purchase.
--
-- Note: This file is reference documentation only. The actual CREATE TABLE
-- statement is inline in jobs/spark/analytics_incremental.py.
-- =============================================================================

CREATE TABLE IF NOT EXISTS analytics.ga4_funnel_analysis (
    -- Grain
    metric_date                 DATE            COMMENT 'Event date',
    funnel_name                 STRING          COMMENT 'Funnel identifier, e.g. ecommerce',
    step_number                 INT             COMMENT 'Ordinal position of the step, starting at 1',
    step_name                   STRING          COMMENT 'GA4 event name for the step, e.g. add_to_cart',

    -- Step volume
    step_users                  BIGINT          COMMENT 'Distinct users reaching this step',
    step_sessions               BIGINT          COMMENT 'Distinct sessions reaching this step',

    -- Progression (0.0 to 1.0)
    step_completion_rate        DECIMAL(5, 4)   COMMENT 'step_users / users at the previous step',
    dropoff_users               BIGINT          COMMENT 'Users lost between the previous step and this one',
    dropoff_rate                DECIMAL(5, 4)   COMMENT 'dropoff_users / users at the previous step',

    -- Outcome
    total_conversions           BIGINT          COMMENT 'Conversions attributed to this funnel and date',
    total_conversion_value      DECIMAL(18, 2)  COMMENT 'Summed conversion value',

    -- Metadata
    _computed_at                TIMESTAMP       COMMENT 'Computation time',
    _version                    INT             COMMENT 'Schema version'
)
USING iceberg
PARTITIONED BY (funnel_name, months(metric_date))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
