-- =============================================================================
-- Analytics: payment_metrics
-- =============================================================================
-- Aggregated payment metrics from Stripe charges.
-- Enables payment performance analysis and monitoring.
-- =============================================================================

CREATE TABLE IF NOT EXISTS analytics.payment_metrics (
    -- Dimensions
    payment_date                DATE            COMMENT 'Payment date',
    card_brand                  STRING          COMMENT 'Card brand: visa, mastercard, etc.',
    card_funding                STRING          COMMENT 'Card type: credit, debit, prepaid',
    billing_country             STRING          COMMENT 'Billing country',

    -- Charge counts
    total_charges               BIGINT          COMMENT 'Total charge attempts',
    successful_charges          BIGINT          COMMENT 'Successful charges',
    failed_charges              BIGINT          COMMENT 'Failed charges',
    disputed_charges            BIGINT          COMMENT 'Disputed charges',
    full_refunds                BIGINT          COMMENT 'Fully refunded charges',
    partial_refunds             BIGINT          COMMENT 'Partially refunded charges',

    -- Volume metrics
    gross_volume                DECIMAL(18, 2)  COMMENT 'Gross charge volume',
    successful_volume           DECIMAL(18, 2)  COMMENT 'Successfully captured volume',
    refunded_volume             DECIMAL(18, 2)  COMMENT 'Refunded amount',
    net_volume                  DECIMAL(18, 2)  COMMENT 'Net volume after refunds',
    fee_volume                  DECIMAL(18, 2)  COMMENT 'Platform fees collected',

    -- Averages
    avg_charge_amount           DECIMAL(18, 2)  COMMENT 'Average charge amount',
    avg_refund_amount           DECIMAL(18, 2)  COMMENT 'Average refund amount',

    -- Rates
    success_rate                DECIMAL(5, 4)   COMMENT 'Charge success rate',
    refund_rate                 DECIMAL(5, 4)   COMMENT 'Refund rate',
    dispute_rate                DECIMAL(5, 4)   COMMENT 'Dispute rate',

    -- Risk metrics
    avg_risk_score              DECIMAL(5, 2)   COMMENT 'Average risk score',
    high_risk_charges           BIGINT          COMMENT 'High risk charge count',

    -- Customer metrics
    unique_customers            BIGINT          COMMENT 'Unique customers',
    unique_payment_methods      BIGINT          COMMENT 'Unique payment methods',

    -- Metadata
    _computed_at                TIMESTAMP       COMMENT 'Computation time'
)
USING iceberg
PARTITIONED BY (payment_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
