# Engineering Task Breakdown - Multi-Business Expansion

**Project:** Data Pipeline Expansion for 3 Business Lines
**Timeline:** 12 weeks
**Engineers:** 1 Staff + 2 Senior + 1 Data Analyst (part-time)

---

## Phase 1: Foundation & Refactoring (Weeks 1-2)

### Task 1.1: Entity Resolution Refactor
**Owner:** Staff Engineer
**Effort:** 8 days
**Dependencies:** None
**Acceptance Criteria:**
- [ ] Refactor `entity_backfill.py` to use dynamic SQL generation instead of hardcoded LEFT JOINs
- [ ] Create `SourceRegistry` class to manage source metadata
- [ ] Implement `generate_blocking_index_query()` function
- [ ] Maintain backward compatibility with existing 4 sources
- [ ] Unit tests for SQL generation
- [ ] Performance benchmarks (must match or beat current 10-minute runtime)

**Files Modified:**
- `jobs/spark/entity_backfill.py` (major refactor)
- `jobs/spark/source_registry.py` (new)
- `tests/test_entity_resolution.py` (new)

**Technical Notes:**
```python
# Before: Hardcoded LEFT JOINs (doesn't scale)
df = shopify_df.join(stripe_df, ...).join(hubspot_df, ...).join(mailchimp_df, ...)

# After: Dynamic generation
sources = SourceRegistry.get_all_customer_sources()
df = build_blocking_index(spark, sources)
```

---

### Task 1.2: API Polling Framework
**Owner:** Senior Engineer #1
**Effort:** 5 days
**Dependencies:** None
**Acceptance Criteria:**
- [ ] Create `BasePollingOperator` Airflow class
- [ ] Implement rate limiting with exponential backoff
- [ ] Add checkpoint/watermark persistence (incremental polling)
- [ ] Error handling with DLQ (dead letter queue) pattern
- [ ] Monitoring hooks (Prometheus metrics)
- [ ] Unit tests for retry logic

**Files Created:**
- `airflow/plugins/polling_operator.py` (new)
- `airflow/plugins/rate_limiter.py` (new)
- `tests/test_polling_operator.py` (new)

**Configuration:**
```yaml
# infrastructure/.env additions
QUICKBOOKS_POLL_INTERVAL_MINUTES=60
HARVEST_POLL_INTERVAL_MINUTES=60
INVENTORY_PLANNER_POLL_INTERVAL_MINUTES=30
```

---

### Task 1.3: Schema Management System
**Owner:** Senior Engineer #2
**Effort:** 4 days
**Dependencies:** None
**Acceptance Criteria:**
- [ ] Create versioned schema files for 6 new sources
- [ ] JSON Schema validation on webhook ingestion
- [ ] Schema evolution detection in staging layer
- [ ] Schema registry documentation generator
- [ ] Validation tests for all schemas

**Files Created:**
- `schemas/shipstation/*.json` (5 schemas)
- `schemas/inventory_planner/*.json` (4 schemas)
- `schemas/asana/*.json` (6 schemas)
- `schemas/harvest/*.json` (5 schemas)
- `schemas/gumroad/*.json` (4 schemas)
- `schemas/memberful/*.json` (4 schemas)
- `schemas/quickbooks/*.json` (6 schemas)
- `scripts/validate_schemas.py` (new)

---

### Task 1.4: Development Environment Setup
**Owner:** Senior Engineer #1 & #2
**Effort:** 2 days (parallel)
**Dependencies:** None
**Acceptance Criteria:**
- [ ] Create test accounts for all 6 SaaS services
- [ ] Document API authentication setup
- [ ] Create sandbox/test mode configurations
- [ ] Set up webhook forwarding (ngrok/localtunnel)
- [ ] Verify API access and rate limits

**Documentation:**
- `docs/SAAS_SETUP_GUIDE.md` (new)

---

## Phase 2: Financial Data Integration (Weeks 3-4)

### Task 2.1: QuickBooks Online Integration
**Owner:** Senior Engineer #1
**Effort:** 6 days
**Dependencies:** Task 1.2 (polling framework)

**Subtasks:**
1. **API Client (1 day)**
   - [ ] OAuth 2.0 authentication flow
   - [ ] Refresh token management
   - [ ] API client class with rate limiting

2. **Data Extraction (2 days)**
   - [ ] Airflow DAG for polling (6 endpoints)
   - [ ] Incremental sync using `LastUpdatedTime`
   - [ ] Write to `raw.quickbooks_*` tables

3. **Staging Transformations (2 days)**
   - [ ] 6 staging functions in `staging_batch.py`
   - [ ] Data type normalization
   - [ ] Currency conversions (if multi-currency)

4. **Testing (1 day)**
   - [ ] Mock data generator for QuickBooks
   - [ ] End-to-end test with sample data
   - [ ] Validation against QuickBooks UI

**Files:**
- `ingestion/app/clients/quickbooks_client.py` (new)
- `airflow/dags/quickbooks_polling.py` (new)
- `jobs/spark/staging_batch.py` (add functions)
- `datagen/quickbooks_provider.py` (new)

---

### Task 2.2: Harvest Integration
**Owner:** Senior Engineer #2
**Effort:** 5 days
**Dependencies:** Task 1.2 (polling framework)

**Subtasks:**
1. **API Client (1 day)**
   - [ ] Bearer token authentication
   - [ ] Account ID resolution
   - [ ] API client with pagination

2. **Data Extraction (2 days)**
   - [ ] Airflow DAG for polling (5 endpoints)
   - [ ] Incremental sync using `updated_at`
   - [ ] Write to `raw.harvest_*` tables

3. **Staging Transformations (1.5 days)**
   - [ ] 5 staging functions
   - [ ] Project-task hierarchy normalization
   - [ ] Time entry aggregations

4. **Testing (0.5 day)**
   - [ ] Mock data generator
   - [ ] Validation tests

**Files:**
- `ingestion/app/clients/harvest_client.py` (new)
- `airflow/dags/harvest_polling.py` (new)
- `jobs/spark/staging_batch.py` (add functions)
- `datagen/harvest_provider.py` (new)

---

### Task 2.3: Finance Dashboard Development
**Owner:** Data Analyst
**Effort:** 4 days
**Dependencies:** Task 2.1, 2.2 complete

**Subtasks:**
1. **Analytics Layer (2 days)**
   - [ ] `finance_revenue_by_line()` function
   - [ ] `finance_margin_analysis()` function
   - [ ] `finance_cash_flow()` function
   - [ ] `finance_payment_health()` function

2. **Marts Layer (1 day)**
   - [ ] `marts.finance_summary` table
   - [ ] Aggregate daily/weekly/monthly

3. **Grafana Dashboard (1 day)**
   - [ ] 4 panels per KPI (16 total)
   - [ ] Time range selector
   - [ ] Drill-down links

**Files:**
- `jobs/spark/analytics_incremental.py` (add functions)
- `jobs/spark/marts_incremental.py` (add function)
- `monitoring/grafana/dashboards/finance_health.json` (new)

---

### Task 2.4: Phase 2 Integration Testing
**Owner:** Both Senior Engineers
**Effort:** 2 days
**Dependencies:** All Phase 2 tasks

**Acceptance Criteria:**
- [ ] End-to-end test with mock data (500 records each)
- [ ] Validate data in all layers (raw → staging → analytics → marts)
- [ ] Finance dashboard displays correct KPIs
- [ ] Performance benchmark (<4 hour processing time)
- [ ] Error handling verification (API failures, schema changes)

---

## Phase 3: Inventory & Fulfillment (Weeks 5-6)

### Task 3.1: ShipStation Integration (Webhook)
**Owner:** Senior Engineer #1
**Effort:** 5 days
**Dependencies:** None

**Subtasks:**
1. **Webhook Ingestion (1.5 days)**
   - [ ] FastAPI endpoint with signature validation
   - [ ] Route 5 event types to Redpanda topics
   - [ ] Error handling and DLQ

2. **Flink Streaming (1.5 days)**
   - [ ] 5 Flink SQL jobs (*_full.sql)
   - [ ] Write to `raw.shipstation_*` tables
   - [ ] Checkpointing configuration

3. **Staging Transformations (1.5 days)**
   - [ ] 5 staging functions
   - [ ] Shipment status normalization
   - [ ] Tracking event aggregation

4. **Testing (0.5 day)**
   - [ ] Mock webhook simulator
   - [ ] Validation tests

**Files:**
- `ingestion/app/routers/shipstation.py` (new)
- `ingestion/app/validators/shipstation_signatures.py` (new)
- `jobs/flink/shipstation_orders_full.sql` (new, + 4 more)
- `jobs/spark/staging_batch.py` (add functions)
- `datagen/shipstation_poster.py` (new)

---

### Task 3.2: Inventory Planner Integration (API Polling)
**Owner:** Senior Engineer #2
**Effort:** 4 days
**Dependencies:** Task 1.2

**Subtasks:**
1. **API Client (1 day)**
   - [ ] API key authentication
   - [ ] Multi-warehouse support
   - [ ] API client with pagination

2. **Data Extraction (1.5 days)**
   - [ ] Airflow DAG for polling (4 endpoints)
   - [ ] Snapshot-based extraction (no incremental)
   - [ ] Write to `raw.inventory_planner_*` tables

3. **Staging Transformations (1 day)**
   - [ ] 4 staging functions
   - [ ] SKU normalization (match Shopify SKUs)
   - [ ] Stock level aggregations

4. **Testing (0.5 day)**
   - [ ] Mock data generator
   - [ ] Validation tests

**Files:**
- `ingestion/app/clients/inventory_planner_client.py` (new)
- `airflow/dags/inventory_planner_polling.py` (new)
- `jobs/spark/staging_batch.py` (add functions)
- `datagen/inventory_planner_provider.py` (new)

---

### Task 3.3: Inventory Dashboard Development
**Owner:** Data Analyst
**Effort:** 4 days
**Dependencies:** Task 3.1, 3.2 complete

**Subtasks:**
1. **Analytics Layer (2 days)**
   - [ ] `inventory_stock_levels()` function
   - [ ] `inventory_fulfillment_performance()` function
   - [ ] `inventory_health_metrics()` function
   - [ ] `inventory_cost_analysis()` function

2. **Marts Layer (1 day)**
   - [ ] `marts.inventory_health` table
   - [ ] SKU-level aggregations
   - [ ] Warehouse-level rollups

3. **Grafana Dashboard (1 day)**
   - [ ] 4 panels per KPI (16 total)
   - [ ] Alert thresholds (out-of-stock, slow-moving)
   - [ ] SKU drill-down

**Files:**
- `jobs/spark/analytics_incremental.py` (add functions)
- `jobs/spark/marts_incremental.py` (add function)
- `monitoring/grafana/dashboards/inventory_optimization.json` (new)

---

### Task 3.4: Phase 3 Integration Testing
**Owner:** Both Senior Engineers
**Effort:** 2 days
**Dependencies:** All Phase 3 tasks

**Acceptance Criteria:**
- [ ] End-to-end test with mock data
- [ ] Inventory dashboard displays correct metrics
- [ ] ShipStation webhook delivery confirmed
- [ ] Stock level accuracy validation
- [ ] Performance benchmark

---

## Phase 4: Services & Digital Products (Weeks 7-8)

### Task 4.1: Asana Integration (Webhook)
**Owner:** Senior Engineer #1
**Effort:** 5 days
**Dependencies:** None

**Subtasks:**
1. **Webhook Ingestion (1.5 days)**
   - [ ] FastAPI endpoint with secret validation
   - [ ] Route 6 event types to Redpanda topics
   - [ ] Workspace ID filtering

2. **Flink Streaming (1.5 days)**
   - [ ] 6 Flink SQL jobs
   - [ ] Write to `raw.asana_*` tables
   - [ ] Handle nested JSON (custom fields)

3. **Staging Transformations (1.5 days)**
   - [ ] 6 staging functions
   - [ ] Project hierarchy normalization
   - [ ] Task dependency resolution

4. **Testing (0.5 day)**
   - [ ] Mock webhook simulator
   - [ ] Validation tests

**Files:**
- `ingestion/app/routers/asana.py` (new)
- `jobs/flink/asana_projects_full.sql` (new, + 5 more)
- `jobs/spark/staging_batch.py` (add functions)
- `datagen/asana_poster.py` (new)

---

### Task 4.2: Gumroad Integration (Webhook)
**Owner:** Senior Engineer #2
**Effort:** 4 days
**Dependencies:** None

**Subtasks:**
1. **Webhook Ingestion (1 day)**
   - [ ] FastAPI endpoint (no signature, IP whitelist)
   - [ ] Route 4 event types to Redpanda topics

2. **Flink Streaming (1 day)**
   - [ ] 4 Flink SQL jobs
   - [ ] Write to `raw.gumroad_*` tables

3. **Staging Transformations (1.5 days)**
   - [ ] 4 staging functions
   - [ ] Digital product taxonomy
   - [ ] Refund handling

4. **Testing (0.5 day)**
   - [ ] Mock webhook simulator
   - [ ] Validation tests

**Files:**
- `ingestion/app/routers/gumroad.py` (new)
- `jobs/flink/gumroad_sales_full.sql` (new, + 3 more)
- `jobs/spark/staging_batch.py` (add functions)
- `datagen/gumroad_poster.py` (new)

---

### Task 4.3: Memberful Integration (Webhook)
**Owner:** Senior Engineer #2
**Effort:** 4 days
**Dependencies:** None

**Subtasks:**
1. **Webhook Ingestion (1 day)**
   - [ ] FastAPI endpoint with signature validation
   - [ ] Route 4 event types to Redpanda topics

2. **Flink Streaming (1 day)**
   - [ ] 4 Flink SQL jobs
   - [ ] Write to `raw.memberful_*` tables

3. **Staging Transformations (1.5 days)**
   - [ ] 4 staging functions
   - [ ] Subscription lifecycle tracking
   - [ ] Churn analysis prep

4. **Testing (0.5 day)**
   - [ ] Mock webhook simulator
   - [ ] Validation tests

**Files:**
- `ingestion/app/routers/memberful.py` (new)
- `jobs/flink/memberful_members_full.sql` (new, + 3 more)
- `jobs/spark/staging_batch.py` (add functions)
- `datagen/memberful_poster.py` (new)

---

### Task 4.4: Operations Dashboard Development
**Owner:** Data Analyst
**Effort:** 5 days
**Dependencies:** Task 4.1, 4.2, 4.3 complete

**Subtasks:**
1. **Analytics Layer (2.5 days)**
   - [ ] `operations_project_performance()` function
   - [ ] `operations_team_utilization()` function
   - [ ] `operations_digital_engagement()` function
   - [ ] `operations_customer_segmentation()` function

2. **Marts Layer (1.5 days)**
   - [ ] `marts.operations_summary` table
   - [ ] Cross-business customer rollups
   - [ ] Service project aggregations

3. **Grafana Dashboard (1 day)**
   - [ ] 4 panels per KPI (16 total)
   - [ ] Customer 360 view
   - [ ] Project timeline visualization

**Files:**
- `jobs/spark/analytics_incremental.py` (add functions)
- `jobs/spark/marts_incremental.py` (add function)
- `monitoring/grafana/dashboards/operations_efficiency.json` (new)

---

### Task 4.5: Enhanced Entity Resolution
**Owner:** Staff Engineer
**Effort:** 4 days
**Dependencies:** Task 1.1, all Phase 4 integrations

**Subtasks:**
1. **Service Project Entity (1.5 days)**
   - [ ] Link Asana projects → Harvest time → QuickBooks invoices
   - [ ] Project profitability calculation

2. **Digital Product Entity (1.5 days)**
   - [ ] Link Gumroad products → Memberful subscriptions
   - [ ] Product performance metrics

3. **Enhanced Customer Entity (1 day)**
   - [ ] Add 6 new sources to customer resolution
   - [ ] Cross-business customer identification
   - [ ] Customer lifetime value calculation

**Files:**
- `jobs/spark/entity_backfill.py` (extend)
- `jobs/spark/semantic_views.py` (add entities)

---

### Task 4.6: Phase 4 Integration Testing
**Owner:** Both Senior Engineers + Staff Engineer
**Effort:** 2 days
**Dependencies:** All Phase 4 tasks

**Acceptance Criteria:**
- [ ] End-to-end test with mock data
- [ ] Operations dashboard displays correct metrics
- [ ] Entity resolution validates across all 10 sources
- [ ] Cross-business customer linking verified
- [ ] Performance benchmark

---

## Phase 5: Integration & Testing (Weeks 9-10)

### Task 5.1: End-to-End Data Pipeline Test
**Owner:** Staff Engineer
**Effort:** 3 days
**Dependencies:** All Phases 1-4 complete

**Acceptance Criteria:**
- [ ] Generate 10,000 events across all 10 sources
- [ ] Validate data propagation through all 5 layers
- [ ] Verify 3 dashboards display correct metrics
- [ ] Check data lineage (raw → marts)
- [ ] Validate entity resolution accuracy (>95%)

**Test Scenarios:**
1. Multi-business customer journey (e-commerce + service + digital)
2. Service project lifecycle (Asana → Harvest → QuickBooks)
3. Inventory reorder trigger (low stock → purchase order)
4. Digital subscription churn (Memberful cancellation)

---

### Task 5.2: Performance Tuning
**Owner:** Staff Engineer + Senior Engineer #1
**Effort:** 4 days
**Dependencies:** Task 5.1

**Optimization Targets:**
- [ ] Entity resolution runtime <15 minutes (10 sources)
- [ ] Staging batch processing <1 hour
- [ ] Analytics incremental <30 minutes
- [ ] Dashboard query response <2 seconds

**Tuning Activities:**
- Spark partition optimization
- Iceberg table compaction
- ClickHouse materialized views
- Grafana query caching

---

### Task 5.3: Dashboard UAT with Stakeholders
**Owner:** Data Analyst + Product Manager
**Effort:** 3 days
**Dependencies:** Task 5.1

**UAT Process:**
1. **Day 1:** VP Finance review finance dashboard
2. **Day 2:** VP Operations review operations dashboard
3. **Day 3:** Inventory Manager review inventory dashboard

**Feedback Loop:**
- [ ] Collect UI/UX feedback
- [ ] Validate KPI calculations
- [ ] Adjust thresholds/alerts
- [ ] Refine visualizations

---

### Task 5.4: Monitoring & Alerting Setup
**Owner:** Senior Engineer #2
**Effort:** 3 days
**Dependencies:** Task 5.1

**Monitoring Coverage:**
- [ ] Prometheus metrics for all 6 new sources
- [ ] Grafana alerts for data freshness
- [ ] Dead letter queue monitoring
- [ ] API rate limit tracking
- [ ] Entity resolution accuracy tracking

**Alerting Rules:**
- Data lag >6 hours → Slack alert
- API failure rate >5% → PagerDuty
- Entity resolution accuracy <90% → Email

**Files:**
- `monitoring/prometheus/alerts/multi_business.yml` (new)
- `monitoring/grafana/dashboards/pipeline_health.json` (extend)

---

### Task 5.5: Documentation & Runbook
**Owner:** Senior Engineer #1 + Data Analyst
**Effort:** 3 days
**Dependencies:** All Phase 5 tasks

**Documentation Deliverables:**
- [ ] Architecture diagram (updated for 10 sources)
- [ ] Data dictionary (all new tables)
- [ ] Dashboard user guide (3 dashboards)
- [ ] Runbook for common issues
- [ ] API credential rotation procedure

**Files:**
- `docs/ARCHITECTURE_V2.md` (new)
- `docs/DATA_DICTIONARY.md` (extend)
- `docs/DASHBOARD_USER_GUIDE.md` (new)
- `docs/RUNBOOK_MULTI_BUSINESS.md` (new)

---

## Phase 6: Production Rollout (Weeks 11-12)

### Task 6.1: Production Deployment Plan
**Owner:** Staff Engineer
**Effort:** 2 days
**Dependencies:** All Phase 5 tasks complete

**Rollout Strategy:**
- **Week 11:** Financial sources (QuickBooks, Harvest)
- **Week 11.5:** Inventory sources (ShipStation, Inventory Planner)
- **Week 12:** Services/Digital sources (Asana, Gumroad, Memberful)

**Rollback Plan:**
- Feature flags for each source
- Blue-green deployment for critical services
- Database backup before each deployment

---

### Task 6.2: Historical Data Migration
**Owner:** Senior Engineer #1 + #2
**Effort:** 4 days (parallel)
**Dependencies:** Task 6.1

**Migration Scope:**
- Last 90 days for all 6 sources
- Estimated volume: 500K-1M records

**Process:**
1. **Day 1-2:** Extract historical data via APIs
2. **Day 3:** Load to raw layer
3. **Day 4:** Run full pipeline (staging → marts)

**Validation:**
- Row count reconciliation
- Data quality checks
- Dashboard smoke tests

---

### Task 6.3: Production Monitoring (First 2 Weeks)
**Owner:** Staff Engineer + Senior Engineer #2
**Effort:** Ongoing (10 days)
**Dependencies:** Task 6.2

**Monitoring Activities:**
- Daily data quality reports
- API error rate tracking
- Dashboard usage analytics
- Performance regression checks

**Daily Checklist:**
- [ ] All sources ingested successfully
- [ ] Entity resolution completed
- [ ] Dashboards updated
- [ ] No critical alerts

---

### Task 6.4: Stakeholder Training
**Owner:** Data Analyst + Product Manager
**Effort:** 3 days
**Dependencies:** Task 6.1

**Training Sessions:**
1. **Session 1 (1 hour):** Finance dashboard for CFO team
2. **Session 2 (1 hour):** Operations dashboard for ops team
3. **Session 3 (1 hour):** Inventory dashboard for warehouse team

**Training Materials:**
- Video walkthroughs (15 min each)
- Quick reference cards
- FAQ document

---

### Task 6.5: Post-Rollout Retrospective
**Owner:** All Team Members
**Effort:** 1 day
**Dependencies:** Task 6.3 (after 2 weeks monitoring)

**Retrospective Topics:**
- What went well?
- What could be improved?
- Technical debt identified
- Future enhancements prioritized

**Deliverables:**
- Retrospective notes
- Action items for next quarter
- Updated best practices documentation

---

## Resource Allocation Summary

| Phase | Staff Eng | Senior Eng #1 | Senior Eng #2 | Data Analyst |
|-------|-----------|---------------|---------------|--------------|
| 1     | 8 days    | 5 days        | 6 days        | -            |
| 2     | -         | 6 days        | 5 days        | 4 days       |
| 3     | -         | 5 days        | 4 days        | 4 days       |
| 4     | 4 days    | 5 days        | 8 days        | 5 days       |
| 5     | 7 days    | 4 days        | 3 days        | 6 days       |
| 6     | 12 days   | 4 days        | 4 days        | 4 days       |
| **Total** | **31 days** | **29 days** | **30 days** | **23 days** |

**Note:** Data Analyst at 20% allocation across 12 weeks = ~12 days available (work extends longer calendar-wise)

---

## Risk Mitigation Checklist

### Technical Risks
- [ ] Entity resolution performance degradation → Refactor in Phase 1
- [ ] API rate limits exceeded → Implement backoff in Phase 1
- [ ] Flink checkpoint failures → Test thoroughly in Phase 5
- [ ] Data quality issues → Add validation layer in each phase

### Schedule Risks
- [ ] SaaS account setup delays → Start in Week 1
- [ ] Unexpected API limitations → Buffer time in each phase
- [ ] Stakeholder availability → Schedule UAT early

### Operational Risks
- [ ] Production deployment failures → Feature flags + rollback plan
- [ ] Dashboard performance issues → Pre-aggregate in marts layer
- [ ] Monitoring gaps → Comprehensive alerting in Phase 5

---

## Success Criteria

### Phase Completion Gates
- **Phase 1:** Entity resolution refactor passes all tests
- **Phase 2:** Finance dashboard displays correct revenue breakdown
- **Phase 3:** Inventory dashboard shows real-time stock levels
- **Phase 4:** Operations dashboard tracks cross-business customers
- **Phase 5:** E2E test passes with 10K events, all metrics green
- **Phase 6:** Production rollout completed, no P1 incidents

### Project Success Metrics
- All 6 sources integrated and operational
- 3 dashboards deployed and adopted (>80% target user usage)
- Pipeline reliability >99.5%
- Entity resolution accuracy >95%
- Data freshness <4 hours
- Zero data breaches or security incidents

---

## Dependencies & Blockers Log

| Date | Blocker | Impact | Owner | Status | Resolution |
|------|---------|--------|-------|--------|------------|
| TBD  | TBD     | TBD    | TBD   | TBD    | TBD        |

*(Update during standup meetings)*
