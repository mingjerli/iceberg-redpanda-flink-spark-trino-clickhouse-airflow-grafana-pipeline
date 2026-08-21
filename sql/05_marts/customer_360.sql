-- =============================================================================
-- Marts: customer_360
-- =============================================================================
-- Wide, denormalized customer view optimized for BI tools and dashboards.
-- Single table with all customer attributes - no joins required.
-- =============================================================================

CREATE TABLE IF NOT EXISTS marts.customer_360 (
    -- Identity (tokenized -- pii/registry.py)
    customer_id                 STRING          COMMENT 'Unified customer ID',
    email_token                 STRING          COMMENT 'Tokenized primary email',
    phone_token                 STRING          COMMENT 'Tokenized primary phone',
    full_name_token             STRING          COMMENT 'Tokenized full name',
    first_name_token            STRING          COMMENT 'Tokenized first name',
    last_name_token             STRING          COMMENT 'Tokenized last name',

    -- Location
    city                        STRING          COMMENT 'City',
    state                       STRING          COMMENT 'State/province',
    country                     STRING          COMMENT 'Country',
    country_code                STRING          COMMENT 'Country ISO code',
    postal_code                 STRING          COMMENT 'Postal code',

    -- Segmentation
    customer_tier               STRING          COMMENT 'Tier: vip, gold, silver, bronze, new',
    customer_segment            STRING          COMMENT 'Behavioral segment',
    lifecycle_stage             STRING          COMMENT 'Lifecycle stage',
    rfm_segment                 STRING          COMMENT 'RFM segment',

    -- Value metrics
    total_lifetime_value        DECIMAL(18, 2)  COMMENT 'Total lifetime spend',
    estimated_future_value      DECIMAL(18, 2)  COMMENT 'Predicted future value',
    total_orders                BIGINT          COMMENT 'Total order count',
    avg_order_value             DECIMAL(18, 2)  COMMENT 'Average order value',

    -- Recency/Frequency
    first_purchase_date         DATE            COMMENT 'First purchase date',
    last_purchase_date          DATE            COMMENT 'Most recent purchase',
    days_since_last_purchase    INT             COMMENT 'Days since last order',
    purchase_frequency_days     DECIMAL(10, 2)  COMMENT 'Avg days between orders',
    customer_age_days           INT             COMMENT 'Days since first seen',

    -- Engagement scores
    engagement_score            DECIMAL(5, 2)   COMMENT 'Overall engagement 0-100',
    rfm_recency_score           INT             COMMENT 'Recency score 1-5',
    rfm_frequency_score         INT             COMMENT 'Frequency score 1-5',
    rfm_monetary_score          INT             COMMENT 'Monetary score 1-5',

    -- Website engagement
    total_page_views            BIGINT          COMMENT 'Lifetime page views',
    total_sessions              BIGINT          COMMENT 'Lifetime sessions',
    avg_pages_per_session       DECIMAL(10, 2)  COMMENT 'Average pages per session',

    -- Marketing
    accepts_marketing           BOOLEAN         COMMENT 'Marketing consent',
    acquisition_source          STRING          COMMENT 'Original source',
    acquisition_channel         STRING          COMMENT 'Acquisition channel',

    -- Product affinity (could be expanded)
    preferred_currency          STRING          COMMENT 'Most used currency',
    preferred_channel           STRING          COMMENT 'Preferred sales channel',

    -- Cross-system presence
    source_count                INT             COMMENT 'Number of source systems',
    has_shopify_profile         BOOLEAN         COMMENT 'Exists in Shopify',
    has_stripe_profile          BOOLEAN         COMMENT 'Exists in Stripe',
    has_hubspot_profile         BOOLEAN         COMMENT 'Exists in HubSpot',
    has_mailchimp_profile       BOOLEAN         COMMENT 'Exists in Mailchimp',

    -- Source IDs for debugging
    shopify_customer_id         BIGINT          COMMENT 'Shopify ID',
    stripe_customer_id          STRING          COMMENT 'Stripe ID',
    hubspot_contact_id          STRING          COMMENT 'HubSpot ID',
    mailchimp_subscriber_id_token STRING        COMMENT 'Tokenized Mailchimp subscriber ID (pii/registry.py class: mailchimp_id)',

    -- Mailchimp engagement
    mailchimp_status            STRING          COMMENT 'Mailchimp subscription status',
    email_open_rate             DECIMAL(5, 4)   COMMENT 'Average email open rate',
    email_click_rate            DECIMAL(5, 4)   COMMENT 'Average email click rate',
    total_emails_received       BIGINT          COMMENT 'Total emails received',
    total_emails_opened         BIGINT          COMMENT 'Total emails opened',
    total_emails_clicked        BIGINT          COMMENT 'Total emails clicked',
    total_sms_received          BIGINT          COMMENT 'Total SMS received',
    total_sms_clicked           BIGINT          COMMENT 'Total SMS clicked',
    has_sms                     BOOLEAN         COMMENT 'Has active SMS subscription',
    last_email_open_date        DATE            COMMENT 'Last email open date',
    last_email_click_date       DATE            COMMENT 'Last email click date',
    days_since_last_email       INT             COMMENT 'Days since last email open',

    -- Cohorts for analysis
    signup_cohort               STRING          COMMENT 'YYYY-MM signup cohort',
    first_purchase_cohort       STRING          COMMENT 'YYYY-MM first purchase cohort',

    -- Timestamps
    profile_created_at          TIMESTAMP       COMMENT 'Profile creation time',
    profile_updated_at          TIMESTAMP       COMMENT 'Last profile update',
    _refreshed_at               TIMESTAMP       COMMENT 'Mart refresh time'
)
USING iceberg
PARTITIONED BY (customer_segment)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
