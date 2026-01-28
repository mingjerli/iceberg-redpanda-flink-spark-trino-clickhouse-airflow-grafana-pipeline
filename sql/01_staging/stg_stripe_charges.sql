-- =============================================================================
-- Staging: stg_stripe_charges
-- =============================================================================
-- Cleans and standardizes raw Stripe charges for downstream processing.
-- Source: raw.stripe_charges
-- Target: staging.stg_stripe_charges
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging.stg_stripe_charges (
    charge_id                   STRING          COMMENT 'Stripe charge ID',
    customer_id                 STRING          COMMENT 'Stripe customer ID',

    -- Financial (converted from cents to dollars)
    currency                    STRING          COMMENT 'Currency code (uppercase)',
    amount                      DECIMAL(18, 2)  COMMENT 'Charge amount in dollars',
    amount_captured             DECIMAL(18, 2)  COMMENT 'Captured amount',
    amount_refunded             DECIMAL(18, 2)  COMMENT 'Refunded amount',
    net_amount                  DECIMAL(18, 2)  COMMENT 'Net amount (amount - refunded)',
    application_fee_amount      DECIMAL(18, 2)  COMMENT 'Platform fee amount',

    -- Payment method
    payment_method_id           STRING          COMMENT 'Payment method ID',
    payment_method_type         STRING          COMMENT 'Payment type: card, bank_transfer, etc.',
    card_brand                  STRING          COMMENT 'Card brand: visa, mastercard, etc.',
    card_last4                  STRING          COMMENT 'Last 4 digits of card',
    card_exp_month              INT             COMMENT 'Card expiration month',
    card_exp_year               INT             COMMENT 'Card expiration year',
    card_funding                STRING          COMMENT 'Card funding type: credit, debit, prepaid',
    card_country                STRING          COMMENT 'Card issuing country',

    -- Billing details
    billing_name                STRING          COMMENT 'Billing name',
    billing_email               STRING          COMMENT 'Billing email (normalized)',
    billing_phone               STRING          COMMENT 'Billing phone',
    billing_city                STRING          COMMENT 'Billing city',
    billing_country             STRING          COMMENT 'Billing country',
    billing_postal_code         STRING          COMMENT 'Billing postal code',

    -- Status
    status                      STRING          COMMENT 'Charge status: succeeded, failed, pending',
    charge_status               STRING          COMMENT 'Derived status with refund info',
    captured                    BOOLEAN         COMMENT 'Whether charge was captured',
    paid                        BOOLEAN         COMMENT 'Whether charge was paid',
    refunded                    BOOLEAN         COMMENT 'Whether charge was refunded',
    disputed                    BOOLEAN         COMMENT 'Whether charge is disputed',

    -- Derived flags
    is_successful               BOOLEAN         COMMENT 'succeeded AND paid',
    is_refunded                 BOOLEAN         COMMENT 'Any refund applied',
    is_fully_refunded           BOOLEAN         COMMENT 'Fully refunded',
    is_live                     BOOLEAN         COMMENT 'Live mode (not test)',

    -- Failure info
    failure_code                STRING          COMMENT 'Failure code if failed',
    failure_message             STRING          COMMENT 'Failure message',

    -- Risk assessment
    risk_level                  STRING          COMMENT 'Risk level: normal, elevated, highest',
    risk_score                  INT             COMMENT 'Risk score 0-100',
    seller_message              STRING          COMMENT 'Seller-facing message',
    network_status              STRING          COMMENT 'Network response status',

    -- References
    payment_intent_id           STRING          COMMENT 'Associated PaymentIntent ID',
    invoice_id                  STRING          COMMENT 'Associated Invoice ID',
    balance_transaction_id      STRING          COMMENT 'Balance transaction ID',

    -- Display
    statement_descriptor        STRING          COMMENT 'Statement descriptor',
    description                 STRING          COMMENT 'Charge description',
    receipt_url                 STRING          COMMENT 'Receipt URL',

    -- Timestamps
    created_at                  TIMESTAMP       COMMENT 'Charge creation time',

    -- Metadata
    _raw_id                     STRING          COMMENT 'Reference to raw table ID',
    _webhook_event_id           STRING          COMMENT 'Webhook event ID',
    _loaded_at                  TIMESTAMP       COMMENT 'Raw layer load time',
    _staged_at                  TIMESTAMP       COMMENT 'Staging layer process time'
)
USING iceberg
PARTITIONED BY (months(created_at))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
