-- =============================================================================
-- Stripe Charges - Raw Iceberg Table
-- =============================================================================
-- Stores raw charge webhook events from Stripe. This is an append-only table
-- that receives data from Flink streaming jobs.
--
-- Webhook events: charge.succeeded, charge.failed, charge.refunded
-- Source: Stripe REST API (webhook format)
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.stripe_charges (
    -- Primary identifier
    id                              STRING          COMMENT 'Unique charge identifier (ch_xxx)',
    object                          STRING          COMMENT 'Object type, always charge',

    -- Amount fields (in cents)
    amount                          BIGINT          COMMENT 'Charge amount in cents',
    amount_captured                 BIGINT          COMMENT 'Amount captured (may be less if partial)',
    amount_refunded                 BIGINT          COMMENT 'Amount refunded in cents',
    currency                        STRING          COMMENT 'Three-letter currency code (lowercase)',

    -- Customer reference
    customer                        STRING          COMMENT 'Customer ID (cus_xxx)',

    -- Payment details
    payment_intent                  STRING          COMMENT 'PaymentIntent ID (pi_xxx)',
    payment_method                  STRING          COMMENT 'Payment method ID (pm_xxx)',
    payment_method_details          STRING          COMMENT 'Payment method details at transaction time as JSON',

    -- Billing (PII)
    billing_details                 STRING          COMMENT 'Billing information at transaction time as JSON (PII)',
    receipt_email                   STRING          COMMENT 'Email for receipt (PII)',
    shipping                        STRING          COMMENT 'Shipping information as JSON (PII)',

    -- Status
    status                          STRING          COMMENT 'Charge status: succeeded, pending, failed',
    captured                        BOOLEAN         COMMENT 'Whether charge was captured',
    paid                            BOOLEAN         COMMENT 'Whether charge was successfully paid',
    refunded                        BOOLEAN         COMMENT 'Whether fully refunded',
    disputed                        BOOLEAN         COMMENT 'Whether charge is disputed',

    -- Failure info
    failure_code                    STRING          COMMENT 'Error code if charge failed',
    failure_message                 STRING          COMMENT 'Human-readable failure message',
    failure_balance_transaction     STRING          COMMENT 'Balance transaction for failure',

    -- Risk assessment
    outcome                         STRING          COMMENT 'Payment outcome details with risk info as JSON',
    fraud_details                   STRING          COMMENT 'Fraud assessment details as JSON',

    -- Balance and transfer
    balance_transaction             STRING          COMMENT 'Balance transaction ID (txn_xxx)',
    transfer_data                   STRING          COMMENT 'Transfer destination info as JSON',
    transfer_group                  STRING          COMMENT 'Transfer group ID',
    on_behalf_of                    STRING          COMMENT 'Connected account ID',
    source_transfer                 STRING          COMMENT 'Source transfer ID',

    -- Invoice
    invoice                         STRING          COMMENT 'Invoice ID (in_xxx)',

    -- Application (for Connect)
    application                     STRING          COMMENT 'Connect application ID',
    application_fee                 STRING          COMMENT 'Application fee ID',
    application_fee_amount          BIGINT          COMMENT 'Application fee in cents',

    -- Statement descriptor
    statement_descriptor            STRING          COMMENT 'Statement descriptor',
    statement_descriptor_suffix     STRING          COMMENT 'Statement descriptor suffix',
    calculated_statement_descriptor STRING          COMMENT 'Full statement descriptor',

    -- Receipt
    receipt_number                  STRING          COMMENT 'Transaction number on receipt',
    receipt_url                     STRING          COMMENT 'URL to view receipt',

    -- Refunds
    refunds                         STRING          COMMENT 'List of refunds as JSON',
    review                          STRING          COMMENT 'Review ID if flagged',

    -- Deprecated fields (kept for compatibility)
    source                          STRING          COMMENT 'Deprecated: payment source as JSON',

    -- Additional
    description                     STRING          COMMENT 'Charge description',
    livemode                        BOOLEAN         COMMENT 'True for live mode',
    metadata                        STRING          COMMENT 'Key-value pairs for custom data as JSON',

    -- Timestamps
    created                         TIMESTAMP       COMMENT 'Charge creation timestamp',

    -- Ingestion metadata
    _webhook_received_at            TIMESTAMP       COMMENT 'When webhook was received by ingestion API',
    _webhook_event_id               STRING          COMMENT 'Stripe event ID (evt_xxx)',
    _loaded_at                      TIMESTAMP       COMMENT 'When record was loaded into Iceberg'
)
USING iceberg
PARTITIONED BY (months(created))
TBLPROPERTIES (
    'format-version' = '2',
    'write.upsert.enabled' = 'false',
    'write.parquet.compression-codec' = 'zstd'
);

COMMENT ON TABLE raw.stripe_charges IS 'Raw Stripe charge webhook events. Append-only, partitioned by month.';
