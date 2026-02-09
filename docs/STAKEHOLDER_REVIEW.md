# Stakeholder Review - Multi-Business Data Pipeline Expansion

**Review Date:** 2026-02-08
**Document Reviewed:** DESIGN_MULTI_BUSINESS_EXPANSION.md
**Status:** ✅ APPROVED WITH MODIFICATIONS

---

## VP of Finance Review

**Reviewer:** VP of Finance
**Review Date:** 2026-02-08
**Decision:** ✅ APPROVED with comments

### Strengths
1. **Comprehensive revenue visibility** - The breakdown by business line (e-commerce, services, digital) directly addresses our board reporting requirements
2. **Margin analysis** - Service project profitability tracking will help us price services more effectively
3. **QuickBooks integration** - Covers all GL accounts and will eliminate manual reconciliation
4. **Realistic timeline** - 12 weeks is aggressive but achievable with dedicated resources

### Concerns & Questions

#### 🟡 MEDIUM PRIORITY: Cash Flow Forecasting
**Issue:** Finance dashboard includes "cash flow indicators" but lacks forward-looking forecasting

**Feedback:**
> "I need to predict cash flow 30-60 days out, not just see historical AR aging. Can we add predictive metrics based on outstanding invoices and historical payment patterns?"

**Staff Engineer Response:**
- **Action:** Add to Phase 2, Task 2.3
- **Implementation:** New analytics function `finance_cash_flow_forecast()` using 90-day payment history
- **Timeline:** +1 day to Phase 2
- **Status:** ✅ Accepted, will implement

---

#### 🟢 LOW PRIORITY: Multi-Currency Support
**Issue:** No mention of multi-currency handling

**Feedback:**
> "We're planning international expansion next year. Do we need to design for multi-currency now or retrofit later?"

**Staff Engineer Response:**
- **Action:** Design for single currency (USD) now, document extension point
- **Implementation:** Add currency normalization stub in staging layer, QuickBooks API supports exchange rates
- **Timeline:** No impact on current timeline, adds 2 weeks if needed later
- **Status:** ✅ Accepted, defer to future phase

---

#### 🟢 LOW PRIORITY: Audit Trail
**Issue:** Financial data changes need audit logging

**Feedback:**
> "For compliance, we need to track who changed what and when. Is this built in?"

**Staff Engineer Response:**
- **Action:** Iceberg tables automatically track updates via snapshots (built-in time travel)
- **Implementation:** Add audit query examples to runbook
- **Timeline:** No impact
- **Status:** ✅ Accepted, already covered

---

### Approval Checklist
- [x] Revenue breakdown meets reporting requirements
- [x] Margin analysis supports pricing decisions (with forecasting addition)
- [x] QuickBooks integration covers all GL accounts
- [x] Dashboard refresh frequency acceptable (4 hours - confirmed adequate)
- [x] Historical data migration plan clear (90 days sufficient for initial launch)

**Final Verdict:** ✅ APPROVED - Proceed with implementation, include cash flow forecasting enhancement

---

## VP of Operations Review

**Reviewer:** VP of Operations
**Review Date:** 2026-02-08
**Decision:** ✅ APPROVED with concerns

### Strengths
1. **Cross-business customer view** - This is exactly what we need to identify upsell opportunities
2. **Inventory optimization** - Real-time stock levels and reorder alerts will reduce stockouts by ~30%
3. **Service utilization tracking** - Finally we'll know which team members are overallocated
4. **Phased rollout** - Reduces risk of operational disruption

### Concerns & Questions

#### 🔴 HIGH PRIORITY: Real-Time Inventory Alerts
**Issue:** 4-hour refresh for inventory dashboard is too slow for operational decisions

**Feedback:**
> "If we're out of stock on a popular item, I need to know within 15 minutes, not 4 hours. Can inventory data refresh faster?"

**Staff Engineer Response:**
- **Action:** Decouple inventory pipeline from 4-hour batch schedule
- **Implementation:**
  - Inventory Planner polling: every 15 minutes (currently 30 min)
  - ShipStation webhooks: real-time (already included)
  - Create separate Airflow DAG for high-frequency inventory updates
  - Update marts.inventory_health every 15 minutes instead of 4 hours
- **Timeline:** +2 days to Phase 3 (Task 3.2 modification)
- **Status:** ✅ Accepted, critical for operations

---

#### 🟡 MEDIUM PRIORITY: Project Budget Tracking
**Issue:** Service project profitability only shows actuals, not budget variance

**Feedback:**
> "We need to track budgeted vs actual costs on service projects. Can we see if a project is going over budget in real-time?"

**Staff Engineer Response:**
- **Action:** Add budget tracking to Harvest integration
- **Implementation:**
  - Harvest API includes project budgets (by hours and fees)
  - Add `operations_project_budget_variance()` analytics function
  - Dashboard panel showing projects >10% over budget (red flag)
- **Timeline:** +1 day to Phase 4 (Task 4.4)
- **Status:** ✅ Accepted, high ROI

---

#### 🟡 MEDIUM PRIORITY: Customer Churn Prediction
**Issue:** Operations dashboard shows churn indicators but no predictive model

**Feedback:**
> "Knowing a customer already churned is too late. Can we predict churn risk before it happens?"

**Staff Engineer Response:**
- **Action:** Add basic churn risk scoring (not full ML model)
- **Implementation:**
  - Simple heuristics: declining order frequency, subscription downgrades, support ticket volume
  - Risk score: High/Medium/Low based on weighted factors
  - Not a machine learning model (out of scope for Phase 1)
- **Timeline:** +2 days to Phase 4 (Task 4.4)
- **Status:** ✅ Accepted, use simple heuristics now, ML in future

---

#### 🟢 LOW PRIORITY: Mobile Dashboard Access
**Issue:** Operations team often in warehouse, need mobile-friendly dashboards

**Feedback:**
> "Can we access these dashboards on tablets/phones?"

**Staff Engineer Response:**
- **Action:** Grafana is responsive by default, test on mobile devices during UAT
- **Implementation:** No code changes needed, just verify during Phase 5 testing
- **Timeline:** No impact
- **Status:** ✅ Accepted, already supported

---

### Approval Checklist
- [x] Inventory metrics support reorder decisions (with 15-min refresh)
- [x] Service profitability tracking adequate (with budget variance addition)
- [x] Cross-business customer view actionable (with churn risk scoring)
- [x] Real-time alerts for critical events (inventory alerts <15 min)
- [x] Team utilization metrics accurate (confirmed with Harvest integration)

**Final Verdict:** ✅ APPROVED - Proceed with implementation, prioritize real-time inventory alerts

---

## Staff Engineer Review

**Reviewer:** Staff Engineer
**Review Date:** 2026-02-08
**Decision:** ✅ APPROVED with technical modifications

### Strengths
1. **Entity resolution refactor** - Dynamic SQL generation is the right approach, prevents LEFT JOIN proliferation
2. **API polling framework** - Reusable pattern will make future integrations faster
3. **Phased approach** - Reduces blast radius of failures
4. **Monitoring coverage** - Comprehensive alerting proposed

### Concerns & Technical Debt

#### 🔴 HIGH PRIORITY: Entity Resolution Performance
**Issue:** 10 sources = potential O(n²) comparison problem in blocking index

**Feedback:**
> "With 10 sources, fuzzy matching on email/phone across all combinations could explode. Current Levenshtein distance approach won't scale past 1M customer records. We need a better blocking strategy."

**Technical Analysis:**
- **Current Approach:** Compare all customer records across all sources (Cartesian product)
- **Performance Projection:** 10 sources × 100K customers each = 1M records → 500B comparisons
- **Runtime Estimate:** 2-3 hours (unacceptable)

**Proposed Solution:**
1. **Implement blocking index with hash bucketing:**
   - Email domain blocking (e.g., @gmail.com, @company.com)
   - Phone area code blocking (first 3 digits)
   - Name initial blocking (first letter last name)
2. **Only compare within blocks (reduces comparisons by ~99%)**
3. **Parallel processing per block (Spark partitioning)**

**Timeline Impact:** +3 days to Phase 1 (Task 1.1)
**Status:** ✅ CRITICAL, must implement

---

#### 🟡 MEDIUM PRIORITY: API Credential Management
**Issue:** 10 sets of API credentials in .env file is a security risk

**Feedback:**
> "We're adding API keys for 6 new services. These should be in a secret manager (AWS Secrets Manager, Vault), not environment variables."

**Proposed Solution:**
1. **Phase 1:** Add secret manager integration (choose AWS Secrets Manager or HashiCorp Vault)
2. **Refactor:** All API clients fetch credentials from secret manager at runtime
3. **Rotation:** Implement automatic credential rotation (30-day cycle)

**Timeline Impact:** +2 days to Phase 1 (new Task 1.5)
**Status:** ✅ Accepted, security first

---

#### 🟡 MEDIUM PRIORITY: Flink Checkpoint Recovery
**Issue:** 4 new Flink jobs, no documented recovery procedure

**Feedback:**
> "If a Flink job fails mid-checkpoint, how do we recover without data loss? We need a runbook section on checkpoint troubleshooting."

**Proposed Solution:**
1. **Document checkpoint recovery steps:**
   - Locate last valid checkpoint in MinIO
   - Restore from checkpoint via Flink CLI
   - Verify Kafka offset alignment
2. **Add monitoring:** Alert if checkpoint interval >10 minutes (indicates backpressure)
3. **Test:** Simulate Flink failure during Phase 5 testing

**Timeline Impact:** +1 day to Phase 5 (Task 5.5 - Runbook)
**Status:** ✅ Accepted, critical for production

---

#### 🟢 LOW PRIORITY: Spark Job Optimization
**Issue:** 34 new staging tables, Spark job runtime could increase significantly

**Feedback:**
> "Current staging_batch.py takes ~45 minutes for 4 sources. With 10 sources, this could hit 2+ hours. We should benchmark and optimize."

**Proposed Solution:**
1. **Benchmark:** Measure current runtime per source
2. **Optimize:** Parallel processing of independent staging functions (use ThreadPoolExecutor)
3. **Target:** Keep total runtime under 1.5 hours

**Timeline Impact:** +2 days to Phase 5 (Task 5.2 - Performance Tuning)
**Status:** ✅ Accepted, important for SLA

---

#### 🟢 LOW PRIORITY: Schema Evolution Handling
**Issue:** SaaS providers change APIs frequently, need automated detection

**Feedback:**
> "When Shopify/Stripe/etc. add a new field, we should detect it automatically rather than failing silently."

**Proposed Solution:**
1. **Schema drift detection:** Compare incoming webhook payload against stored schema
2. **Alert:** Notify via Slack if new fields detected (not critical, just informational)
3. **Logging:** Log all unknown fields for future integration

**Timeline Impact:** +1 day to Phase 1 (Task 1.3)
**Status:** ✅ Accepted, low effort high value

---

### Approval Checklist
- [x] Entity resolution refactor approach sound (with blocking index enhancement)
- [x] API polling framework scalable (confirmed)
- [x] Error handling & retry logic robust (exponential backoff included)
- [x] Monitoring & observability sufficient (comprehensive)
- [x] Tech debt addressed (dynamic SQL generation solves LEFT JOIN issue)

**Additional Requirements:**
- [x] Add blocking index optimization to entity resolution
- [x] Implement secret manager for API credentials
- [x] Document Flink checkpoint recovery
- [x] Benchmark and optimize Spark performance
- [x] Add schema drift detection

**Final Verdict:** ✅ APPROVED - Proceed with technical enhancements included

---

## Product Manager Review

**Reviewer:** Product Manager
**Review Date:** 2026-02-08
**Decision:** ✅ APPROVED with UX refinements

### Strengths
1. **User-centric dashboard design** - KPIs align with actual decision-making workflows
2. **Phased rollout** - Allows for iterative feedback and adjustments
3. **Stakeholder training** - Often overlooked, critical for adoption
4. **Clear success metrics** - 80% adoption target is measurable and realistic

### Concerns & UX Improvements

#### 🟡 MEDIUM PRIORITY: Dashboard Discoverability
**Issue:** 3 separate dashboards, users might not know which one to use

**Feedback:**
> "How do users know which dashboard to open? We need a landing page or dashboard selector."

**Proposed Solution:**
1. **Create Grafana home dashboard:** "Data Pipeline Hub"
   - Tiles for each of the 3 dashboards (Finance, Operations, Inventory)
   - Quick stats preview (revenue, stock alerts, project count)
   - Direct links to detailed dashboards
2. **Set as default home page** for all users

**Timeline Impact:** +1 day to Phase 5 (Task 5.3 - UAT)
**Status:** ✅ Accepted, improves UX

---

#### 🟡 MEDIUM PRIORITY: Alert Notification Preferences
**Issue:** Alerts go to Slack/PagerDuty, but users have different preferences

**Feedback:**
> "VP Finance wants email, Inventory Manager wants SMS, I want Slack. Can we customize notification channels per user?"

**Proposed Solution:**
1. **Phase 1:** Default to Slack for all alerts (consistent)
2. **Phase 7 (Future):** Implement user notification preferences
3. **Workaround:** Use Slack workflow automation to route to email/SMS if needed

**Timeline Impact:** No impact (defer to post-launch)
**Status:** ✅ Accepted, defer customization

---

#### 🟢 LOW PRIORITY: Dashboard Export to PDF
**Issue:** VP Finance needs to include dashboard snapshots in board decks

**Feedback:**
> "Can we export dashboard panels to PDF for presentations?"

**Proposed Solution:**
- Grafana Enterprise supports PDF export, but we're using OSS version
- **Workaround:** Screenshot via browser print-to-PDF (works fine)
- **Alternative:** Upgrade to Grafana Enterprise later if needed ($$$)

**Timeline Impact:** None
**Status:** ✅ Accepted, workaround sufficient

---

#### 🟢 LOW PRIORITY: Dashboard Annotations
**Issue:** When we run promotions or events, we want to annotate dashboard timelines

**Feedback:**
> "Can we add notes like 'Black Friday Sale' to the dashboard timeline so we can correlate revenue spikes?"

**Proposed Solution:**
- Grafana supports annotations natively
- **Implementation:** Add annotation API endpoint, users can add via UI
- **Training:** Include in stakeholder training (Phase 6, Task 6.4)

**Timeline Impact:** None (built-in feature)
**Status:** ✅ Accepted, include in training

---

### Approval Checklist
- [x] User stories for each dashboard validated
- [x] Data definitions consistent across views
- [x] MVP scope appropriate for 12-week timeline (confirmed)
- [x] Phased rollout minimizes risk (financial → inventory → services)
- [x] Stakeholder training plan included (3 sessions, 1 hour each)

**Final Verdict:** ✅ APPROVED - Proceed with UX enhancements, prioritize dashboard hub

---

## Senior Engineer Review (Implementation Feasibility)

**Reviewer:** Senior Engineers #1 & #2
**Review Date:** 2026-02-08
**Decision:** ✅ APPROVED with resource allocation adjustments

### Implementation Feedback

#### 🟡 MEDIUM PRIORITY: Task Dependencies Clarification
**Issue:** Some tasks listed as parallel but have hidden dependencies

**Feedback (Senior #1):**
> "Task 2.1 (QuickBooks) and Task 2.2 (Harvest) are listed as parallel, but both depend on the polling framework (Task 1.2). We can't start both simultaneously. Need to clarify critical path."

**Proposed Solution:**
- **Update project tracker:** Add explicit dependency arrows
- **Gantt chart:** Create visual timeline showing task dependencies
- **Daily standup:** Coordinate task handoffs between engineers

**Timeline Impact:** None (just clarification)
**Status:** ✅ Accepted, update tracker

---

#### 🟡 MEDIUM PRIORITY: Testing Bandwidth
**Issue:** Integration testing tasks are 2 days each, but that's aggressive for 6+ sources

**Feedback (Senior #2):**
> "Phase 2-4 integration testing (Tasks 2.4, 3.4, 4.6) are each 2 days. With mock data generation, validation, and debugging, I think we need 3 days per phase."

**Proposed Solution:**
- **Extend testing tasks:** +1 day each (3 phases = +3 days total)
- **Total timeline:** 12 weeks → 13 weeks (still acceptable)
- **Buffer:** Keeps original 12-week timeline by using contingency

**Timeline Impact:** +3 days (absorbed by contingency buffer)
**Status:** ✅ Accepted, realistic estimate

---

#### 🟢 LOW PRIORITY: Code Review Process
**Issue:** No mention of code review in task breakdown

**Feedback (Senior #1):**
> "Should we review each other's code before merging? Who reviews Staff Engineer's entity resolution refactor?"

**Proposed Solution:**
- **PR review policy:** All PRs require 1 approval from another engineer
- **Staff Engineer PRs:** Reviewed by both Senior Engineers
- **Senior Engineer PRs:** Reviewed by each other or Staff Engineer
- **Time allocation:** Factor in ~10% time for code reviews

**Timeline Impact:** None (built into task estimates)
**Status:** ✅ Accepted, add to workflow doc

---

### Resource Allocation Concerns

**Feedback (Both Seniors):**
> "Data Analyst is at 20% allocation but has critical path tasks (dashboard development). If they're blocked or unavailable, we have no backup. Can we increase to 40% or have Senior #2 as backup?"

**Proposed Solution:**
1. **Increase Data Analyst allocation:** 20% → 30% (compromise)
2. **Cross-training:** Senior #2 learns Grafana dashboard creation during Phase 2
3. **Backup plan:** Senior #2 can complete dashboard tasks if analyst unavailable

**Status:** ✅ Accepted, data analyst agrees to 30%

---

### Approval Checklist
- [x] Task breakdown is feasible with current team
- [x] Timeline is realistic (13 weeks with buffer)
- [x] Dependencies clearly documented
- [x] Code review process defined
- [x] Resource allocation adequate (with Data Analyst increase)

**Final Verdict:** ✅ APPROVED - Proceed with implementation, extend testing phases

---

## Consolidated Changes to Plan

### Timeline Adjustments
| Phase | Original | Adjusted | Reason |
|-------|----------|----------|--------|
| Phase 1 | 2 weeks | 2.5 weeks | +3 days for blocking index + secret manager + schema drift |
| Phase 2 | 2 weeks | 2 weeks | +1 day cash flow forecasting (absorbed by parallel work) |
| Phase 3 | 2 weeks | 2.5 weeks | +2 days for 15-min inventory refresh + 1 day testing |
| Phase 4 | 2 weeks | 2.5 weeks | +3 days for budget variance + churn scoring + 1 day testing |
| Phase 5 | 2 weeks | 2.5 weeks | +2 days Spark optimization + 1 day dashboard hub |
| Phase 6 | 2 weeks | 2 weeks | No change |
| **Total** | **12 weeks** | **14 weeks** | +2 weeks for enhancements + testing |

**Final Timeline:** 14 weeks (within acceptable range, VP Ops approved)

---

### Budget Adjustments
| Item | Original | Adjusted | Reason |
|------|----------|----------|--------|
| SaaS Subscriptions | $12,000 | $12,000 | No change |
| Infrastructure | $1,200 | $1,500 | +$300 for secret manager (AWS Secrets Manager) |
| Contingency | $1,320 | $1,350 | 10% of new total |
| **Total** | **$14,520** | **$14,850** | +$330 |

**Final Budget:** $14,850 (2.3% increase, approved by VP Finance)

---

### New Tasks Added
1. **Task 1.5:** Secret Manager Integration (2 days, Staff Engineer)
2. **Task 2.3.1:** Cash Flow Forecasting Function (1 day, Data Analyst)
3. **Task 3.2.1:** High-Frequency Inventory Polling (1 day, Senior #2)
4. **Task 4.4.1:** Project Budget Variance Tracking (1 day, Data Analyst)
5. **Task 4.4.2:** Basic Churn Risk Scoring (1 day, Senior #1)
6. **Task 5.3.1:** Dashboard Hub Landing Page (1 day, Data Analyst)

---

## Final Approval Status

| Stakeholder | Decision | Date | Conditions |
|-------------|----------|------|------------|
| VP of Finance | ✅ APPROVED | 2026-02-08 | Include cash flow forecasting |
| VP of Operations | ✅ APPROVED | 2026-02-08 | 15-min inventory refresh critical |
| Staff Engineer | ✅ APPROVED | 2026-02-08 | Implement blocking index + secret manager |
| Product Manager | ✅ APPROVED | 2026-02-08 | Add dashboard hub for UX |
| Senior Engineer #1 | ✅ APPROVED | 2026-02-08 | Extend testing phases |
| Senior Engineer #2 | ✅ APPROVED | 2026-02-08 | Cross-train on Grafana |

**Overall Status:** ✅ **APPROVED TO PROCEED**

**Conditions Met:**
- All critical concerns addressed
- Timeline extended to 14 weeks (acceptable)
- Budget increase approved (+$330)
- Resource allocation adjusted (Data Analyst 30%)

---

## Next Steps (Action Items)

### Immediate (This Week)
1. [x] Update `DESIGN_MULTI_BUSINESS_EXPANSION.md` with approved changes
2. [x] Update `TASK_BREAKDOWN_MULTI_BUSINESS.md` with new tasks
3. [x] Update `PROJECT_TRACKER.md` with 14-week timeline
4. [ ] Create Gantt chart with task dependencies
5. [ ] Schedule kickoff meeting (all stakeholders) - **Target: Next Monday**

### Week 1 (Project Start)
6. [ ] Staff Engineer begins Task 1.1 (Entity Resolution Refactor with Blocking Index)
7. [ ] Senior #1 begins Task 1.2 (API Polling Framework)
8. [ ] Senior #2 begins Task 1.3 (Schema Management) + Task 1.5 (Secret Manager)
9. [ ] Data Analyst prepares mock data requirements
10. [ ] Product Manager finalizes dashboard wireframes (UX review)

---

## Meeting Notes

**Kickoff Meeting Agenda (Proposed):**
1. **Welcome & Introductions** (5 min)
2. **Project Overview** (10 min) - Staff Engineer presents design
3. **Timeline & Milestones** (10 min) - Review updated 14-week plan
4. **Roles & Responsibilities** (10 min) - Clear RACI matrix
5. **Communication Plan** (5 min) - Slack channel, standup schedule, status reports
6. **Q&A** (10 min)
7. **Go/No-Go Decision** (5 min)

**Attendees:**
- VP of Finance
- VP of Operations
- Staff Engineer (Lead)
- Senior Engineer #1
- Senior Engineer #2
- Data Analyst
- Product Manager

**Duration:** 1 hour
**Location:** Conference Room / Zoom
**Date:** TBD (pending scheduling)

---

**Document Version:** 1.0
**Status:** ✅ APPROVED
**Next Review:** End of Phase 1 (Week 2.5)
