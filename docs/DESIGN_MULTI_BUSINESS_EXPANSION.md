# Multi-Business Line Data Integration Plan

**Document Status:** Draft for Stakeholder Review
**Date:** 2026-02-08
**Author:** Data Engineering Team

---

## Executive Summary

This plan extends our existing data pipeline (Shopify, Stripe, HubSpot, Mailchimp) to support three business lines:
1. **E-commerce with Inventory** - Physical product sales requiring stock management
2. **Professional Services** - Event planning and service delivery
3. **Digital Products** - Digital art, subscriptions, and downloadable content

**Key Additions:**
- 6 new SaaS integrations
- 3 new Grafana dashboards (Finance, Operations, Inventory)
- Enhanced entity resolution for multi-business customer tracking
- ~42 new tables across all pipeline layers

**Timeline Estimate:** 8-12 weeks
**Risk Level:** Medium (complexity in cross-business entity resolution)

---

## Part 1: Business Requirements & SaaS Service Selection

### 1.1 E-Commerce with Inventory

**Business Needs:**
- Real-time inventory tracking across warehouses
- Stock level alerts and reorder points
- SKU-level profitability analysis
- Fulfillment and shipping tracking

**Recommended SaaS:**
- **ShipStation** (fulfillment & shipping) - Already have Shopify, ShipStation complements
- **Inventory Planner** or **Katana MRP** (inventory optimization)
- **QuickBooks Commerce** (inventory accounting integration)

**For This Plan, We'll Integrate:**
- **ShipStation** - Webhooks for order fulfillment, shipment tracking, label creation
- **Inventory Planner** - API polling for stock levels, reorder recommendations

### 1.2 Professional Services (Event Planning)

**Business Needs:**
- Project/event tracking (milestones, deliverables)
- Time tracking and resource allocation
- Client communication logs
- Service profitability by project type

**Recommended SaaS:**
- **Asana** or **Monday.com** (project management)
- **Harvest** or **Toggl Track** (time tracking)
- **Calendly** (scheduling)

**For This Plan, We'll Integrate:**
- **Asana** - Webhooks for project updates, task completion, milestone tracking
- **Harvest** - API polling for time entries, project budgets, invoices

### 1.3 Digital Products

**Business Needs:**
- Digital asset delivery tracking
- Subscription lifecycle (trials, renewals, churn)
- Download analytics
- Content access patterns

**Recommended SaaS:**
- **Gumroad** or **Patreon** (digital product sales)
- **Memberful** or **Podia** (subscription management)

**For This Plan, We'll Integrate:**
- **Gumroad** - Webhooks for sales, refunds, subscription events
- **Memberful** - Webhooks for member signups, plan changes, cancellations

### 1.4 Financial & Operational Foundation

**Already Integrated:**
- Stripe (payments) ✓
- HubSpot (CRM) ✓
- Mailchimp (marketing) ✓

**Additions:**
- **QuickBooks Online** - API polling for GL entries, invoices, expenses (financial reporting)

---

## Part 2: Dashboard Requirements by Stakeholder

### 2.1 VP of Finance Dashboard

**"Financial Health & Profitability"**

**KPIs:**
1. **Revenue Breakdown by Business Line**
   - E-commerce revenue
   - Professional services revenue
   - Digital product revenue
   - Month-over-month growth rates

2. **Margin Analysis**
   - Gross margin by product category
   - Service project profitability
   - Digital product contribution margin

3. **Cash Flow Indicators**
   - Accounts receivable aging
   - Inventory carrying costs
   - Outstanding project invoices

4. **Payment Health**
   - Stripe payment success rates
   - Refund rates by business line
   - Chargeback trends

**Data Sources:** Stripe, QuickBooks, ShipStation, Harvest, Gumroad, Memberful

**Refresh Frequency:** 4 hours (aligned with existing pipeline)

---

### 2.2 VP of Operations Dashboard

**"Operational Efficiency & Resource Utilization"**

**KPIs:**
1. **Inventory Operations**
   - Stock turnover ratio
   - Out-of-stock events
   - Days of inventory on hand
   - Reorder point alerts

2. **Service Delivery**
   - Active projects by stage
   - Team utilization rates (billable hours %)
   - Project completion velocity
   - Average project margin

3. **Digital Fulfillment**
   - Digital delivery success rate
   - Subscription retention rate
   - Average download time (performance)

4. **Cross-Business Customer View**
   - Multi-business customers (% buying from 2+ lines)
   - Customer lifetime value by segment
   - Churn risk indicators

**Data Sources:** Inventory Planner, ShipStation, Asana, Harvest, Gumroad, Memberful, HubSpot

**Refresh Frequency:** 4 hours

---

### 2.3 Inventory Manager Dashboard

**"Stock & Fulfillment Optimization"**

**KPIs:**
1. **Stock Levels**
   - Current inventory by SKU
   - Safety stock vs actual
   - Slow-moving inventory (>90 days)

2. **Fulfillment Performance**
   - Orders awaiting shipment (aging)
   - Average fulfillment time
   - Shipping carrier performance

3. **Inventory Health**
   - Dead stock value
   - Stock-out impact (lost revenue)
   - Reorder recommendations

4. **Cost Metrics**
   - Cost of goods sold (COGS) by SKU
   - Warehouse storage costs
   - Fulfillment costs per order

**Data Sources:** Inventory Planner, ShipStation, QuickBooks, Shopify

**Refresh Frequency:** 2 hours (more frequent for operational needs)

---

## Part 3: Technical Architecture Design

### 3.1 New Source Integration Pattern

Following existing pattern (Shopify, Stripe, HubSpot, Mailchimp):

**For Each New Source (6 sources):**

1. **Webhook Sources (ShipStation, Asana, Gumroad, Memberful):**
   - FastAPI endpoint with signature validation
   - Route to dedicated Redpanda topic
   - Flink streaming job to `raw.*` Iceberg table
   - Spark staging transformation

2. **API Polling Sources (Inventory Planner, Harvest, QuickBooks):**
   - Airflow scheduled jobs (every 30-60 minutes)
   - Direct write to `raw.*` Iceberg tables
   - Spark staging transformation

### 3.2 Data Layer Expansion

**Current State:** 4 sources × 5-6 tables = ~20 staging tables

**Proposed Addition:**

```
ShipStation:     5 tables (orders, shipments, carriers, labels, tracking_events)
Inventory Planner: 4 tables (stock_levels, reorder_points, purchase_orders, forecasts)
Asana:           6 tables (projects, tasks, users, task_assignments, milestones, comments)
Harvest:         5 tables (time_entries, projects, tasks, invoices, expenses)
Gumroad:         4 tables (sales, products, subscribers, subscription_events)
Memberful:       4 tables (members, plans, subscriptions, subscription_events)
QuickBooks:      6 tables (accounts, transactions, invoices, bills, items, customers)
```

**Total New Tables:** 34 staging tables

**Entity Resolution Expansion:**
- Add "service_project_id" entity type (Asana projects → Harvest time → Invoices)
- Add "digital_product_id" entity type (Gumroad products → Memberful subscriptions)
- Enhance customer entity resolution across all 10 sources

**Analytics Layer:**
- 8 new analytics functions (inventory_metrics, service_performance, digital_engagement, etc.)

**Marts Layer:**
- 3 new marts (finance_summary, operations_summary, inventory_health)

---

## Part 4: Engineering Impact Assessment

### 4.1 File Changes Estimate

Based on existing pattern (Mailchimp = ~14 new + ~22 modified):

**New Files (per source × 6 sources):**
- Flink SQL job (if webhook)
- Signature validation module
- Staging functions
- Analytics functions
- Mock data generator
- Schema definitions

**Modified Files:**
- `entity_backfill.py` - Add LEFT JOINs for each source (tech debt warning: 10 sources)
- `reset_and_run.sh` - Add loops for new sources (8+ locations)
- Airflow DAG - Add tasks for new sources
- FastAPI router - Add webhook endpoints

**Estimated Changes:**
- 84 new files (14 × 6)
- 132 modified files (22 × 6)

### 4.2 Infrastructure Requirements

**Compute:**
- Flink: No change (4 webhook sources fit in current slots)
- Spark: +2 executor cores (more staging tables)
- Airflow: +6 scheduled jobs (API polling)

**Storage:**
- MinIO: +500GB estimated (assuming 6 months retention)

**Redpanda:**
- +4 topics (webhook sources)
- Partition strategy: 3 partitions per topic

### 4.3 Risk Assessment

**HIGH RISK:**
1. **Entity Resolution Complexity**
   - 10 sources = 10 LEFT JOINs in blocking index
   - Performance degradation risk
   - **Mitigation:** Refactor to dynamic join generation

2. **API Rate Limits**
   - QuickBooks: 500 requests/minute
   - Asana: 1500 requests/minute
   - **Mitigation:** Implement exponential backoff

**MEDIUM RISK:**
1. **Data Quality Variance**
   - Different sources = different data quality
   - **Mitigation:** Add data quality checks in staging layer

2. **Schema Evolution**
   - SaaS providers change APIs
   - **Mitigation:** Version all schema definitions

**LOW RISK:**
1. **Dashboard Performance**
   - 3 new dashboards on existing mart tables
   - **Mitigation:** Pre-aggregate in marts layer

---

## Part 5: Implementation Phases

### Phase 1: Foundation (Weeks 1-2)
- Refactor entity resolution for scalability
- Add API polling framework to Airflow
- Create base schemas for 6 new sources
- Set up dev environments for new integrations

### Phase 2: Financial Data (Weeks 3-4)
- Integrate QuickBooks Online
- Integrate Harvest (time tracking)
- Build finance dashboard
- Validation & testing

### Phase 3: Inventory & Fulfillment (Weeks 5-6)
- Integrate ShipStation
- Integrate Inventory Planner
- Build inventory dashboard
- Validation & testing

### Phase 4: Services & Digital (Weeks 7-8)
- Integrate Asana (project management)
- Integrate Gumroad (digital sales)
- Integrate Memberful (subscriptions)
- Build operations dashboard

### Phase 5: Integration & Testing (Weeks 9-10)
- End-to-end testing with mock data
- Cross-business entity resolution validation
- Dashboard UAT with stakeholders
- Performance tuning

### Phase 6: Production Rollout (Weeks 11-12)
- Production deployment (rolling by source)
- Monitoring & alerting setup
- Documentation & runbook updates
- Stakeholder training

---

## Part 6: Stakeholder Review Checklist

### VP of Finance - Review Points
- [ ] Revenue breakdown meets reporting requirements
- [ ] Margin analysis supports pricing decisions
- [ ] QuickBooks integration covers all GL accounts
- [ ] Dashboard refresh frequency acceptable (4 hours)
- [ ] Historical data migration plan clear

### VP of Operations - Review Points
- [ ] Inventory metrics support reorder decisions
- [ ] Service profitability tracking adequate
- [ ] Cross-business customer view actionable
- [ ] Real-time alerts for critical events
- [ ] Team utilization metrics accurate

### Staff Engineer - Review Points
- [ ] Entity resolution refactor approach sound
- [ ] API polling framework scalable
- [ ] Error handling & retry logic robust
- [ ] Monitoring & observability sufficient
- [ ] Tech debt addressed (LEFT JOIN proliferation)

### Product Manager - Review Points
- [ ] User stories for each dashboard validated
- [ ] Data definitions consistent across views
- [ ] MVP scope appropriate for 12-week timeline
- [ ] Phased rollout minimizes risk
- [ ] Stakeholder training plan included

---

## Part 7: Open Questions for Decision

1. **Entity Resolution Refactor:**
   - Option A: Dynamic SQL generation for LEFT JOINs
   - Option B: Separate resolution per business line
   - Option C: Graph-based entity resolution (future-proof)
   - **Recommendation:** Option A (pragmatic, 2-week refactor)

2. **API Polling Frequency:**
   - QuickBooks/Harvest: Every 30 min vs 60 min?
   - **Recommendation:** 60 min (aligns with 4-hour batch window)

3. **Historical Data Migration:**
   - Full historical load (12+ months) vs last 90 days?
   - **Recommendation:** Last 90 days initially, historical load as Phase 7

4. **Dashboard Access Control:**
   - Grafana native auth vs SSO (Okta, Auth0)?
   - **Recommendation:** SSO if already in use, else Grafana teams

5. **Data Retention Policy:**
   - Raw layer: 90 days vs 180 days?
   - Marts layer: Indefinite?
   - **Recommendation:** 180 days raw, indefinite marts

---

## Next Steps

1. **Immediate (This Week):**
   - [ ] VP Finance & VP Operations review business requirements (Part 1-2)
   - [ ] Staff Engineer review technical design (Part 3-4)
   - [ ] Product Manager validate dashboard specs (Part 2)

2. **Week 2:**
   - [ ] Senior Engineer create detailed task breakdown (see TASK_BREAKDOWN.md)
   - [ ] Resolve open questions (Part 7)
   - [ ] Finalize timeline & resource allocation

3. **Week 3:**
   - [ ] Kickoff meeting with all stakeholders
   - [ ] Begin Phase 1 implementation
   - [ ] Set up weekly progress tracking

---

## Appendix A: Cost Estimate

**SaaS Subscriptions (Monthly):**
- QuickBooks Online: $50-200/mo (plan dependent)
- Harvest: $12/user/mo (~5 users = $60/mo)
- Asana: $25/user/mo (~10 users = $250/mo)
- Inventory Planner: $199-499/mo
- Gumroad: 10% transaction fee (no monthly)
- Memberful: $25-100/mo (plan dependent)

**Total Monthly SaaS:** ~$784-1,209/mo

**Infrastructure (AWS/Cloud):**
- Additional storage: ~$20/mo
- Additional compute: ~$50/mo

**Engineering Time:**
- 1 Staff Engineer (30% allocation) × 12 weeks
- 2 Senior Engineers (full-time) × 12 weeks
- 1 Data Analyst (20% allocation) × 12 weeks

---

## Appendix B: Success Metrics

**Technical Metrics:**
- Pipeline reliability: >99.5% uptime
- Data freshness: <4 hours lag
- Entity resolution accuracy: >95%

**Business Metrics:**
- Dashboard adoption: 80% of target users within 4 weeks
- Decision velocity: 20% reduction in time-to-insight
- Data quality: <1% error rate in marts layer

**Operational Metrics:**
- Mean time to detect (MTTD) data issues: <30 minutes
- Mean time to resolve (MTTR) data issues: <2 hours
- Cost per processed event: <$0.001
