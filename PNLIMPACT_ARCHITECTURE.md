# eCommerce P&L Impact Analysis & Simulation — Architecture
### Channel Hierarchy Changes · New Store Onboarding · Walmart Finance Engineering

> **Core question this system answers:**
> *"When a channel hierarchy changes or a new store goes live — what is the P&L impact,
> and how does it compare to what we predicted?"*

> **Confirmed decisions:**
> - `CHANNEL_HIERARCHY_MASTER` exists in BQ ✅
> - No Anaplan integration needed ✅
> - 52-week baseline with seasonal decomposition ✅
> - Two-gate approval: Business then Engineering ✅
> - Fulfillment cost source: TBD ⬜

---

## Table of Contents

1. [What is a Channel Hierarchy Change?](#1-what-is-a-channel-hierarchy-change)
2. [P&L Components in Scope](#2-pl-components-in-scope)
3. [System Overview](#3-system-overview)
4. [Phase 1 — Change Detection](#4-phase-1--change-detection)
5. [Phase 2 — Baseline Computation](#5-phase-2--baseline-computation)
6. [Phase 3 — Forward Simulation (Pre-launch)](#6-phase-3--forward-simulation-pre-launch)
7. [Phase 4 — Actual Impact Measurement (Post-launch)](#7-phase-4--actual-impact-measurement-post-launch)
8. [Phase 5 — Simulation vs Actual Reconciliation](#8-phase-5--simulation-vs-actual-reconciliation)
9. [Two-Gate Approval Workflow](#9-two-gate-approval-workflow)
10. [AI Layer](#10-ai-layer)
11. [Data Model & Schemas](#11-data-model--schemas)
12. [Output Surfaces](#12-output-surfaces)
13. [Implementation Phases](#13-implementation-phases)
14. [Open Questions](#14-open-questions)

---

## 1. What is a Channel Hierarchy Change?

In Walmart eCommerce, orders flow through a **channel hierarchy**:

```
Business Unit  (Walmart.com)
  └── Channel  (Marketplace / First-Party / WFS / Club)
        └── Fulfillment Type  (1P / 3P / WFS / Club Pickup)
              └── Store / Facility Node  (store_id, club_nbr, FC_id)
                    └── Seller / Category
```

A **hierarchy change** is any mutation to this tree:

| Change Type | Example | P&L Impact |
|-------------|---------|-----------|
| **New store node** | New FC goes live (`store_id = 9876`) | Orders attribute to new node; volume, cost, commission land in a new bucket |
| **Reclassification** | FC re-typed from `1P → WFS` | Commission rate changes; fulfillment cost bucket changes |
| **Channel split** | Marketplace → `Marketplace Ads` + `Marketplace Base` | Revenue lines bifurcate; historical comparisons break |
| **Store merged / retired** | Two regional FCs consolidated | Orders reroute; P&L appears to shift between nodes |
| **New seller onboarded** | New 3P seller on marketplace | No refund rate or commission baseline exists yet |

Each change creates **two problems**:
1. **Forward (pre-launch):** What will this do to P&L? → Simulation
2. **Backward (post-launch):** How much of the observed P&L change is actually due to this? → Causal measurement

---

## 2. P&L Components in Scope

Built on top of existing FDL_CoreFinance pipeline tables:

```
GROSS MERCHANDISE VALUE (GMV)
  Source: WM_SALES_ORDER_INV_CHRG_DTL

NET REVENUE
  = GMV
  − OMS_REFUND    (refunds via Order Management System)
  − RAP_REFUND    (refunds via Returns & Adjustments Platform)
  − CB_REFUND     (chargeback refunds)
  − ADJUSTMENT    (post-order price corrections)
  Source: WM_SALES_ORDER_INV_CHRG_DTL (event_nm dimension)

COMMISSION / TAKE RATE  [Marketplace only]
  = GMV × commission_rate(channel, seller, category)
  Source: CHANNEL_HIERARCHY_MASTER × WM_SALES_ORDER_INV_CHRG_DTL

FULFILLMENT COST
  Source: TBD ⬜ (SAP GL via sub-ledger API or separate BQ table)

CONTRIBUTION MARGIN (CM)
  = Net Revenue − Fulfillment Cost − Promotions

UNIT ECONOMICS
  = CM ÷ order_count  (per-order profitability)
```

---

## 3. System Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│  DATA SOURCES                                                            │
│  CHANNEL_HIERARCHY_MASTER (BQ) · WM_SALES_ORDER_INV_CHRG_DTL (BQ/Hudi) │
│  WM_SALES_ORDER_INV_TNDR_DTL · ETL_LOAD_PARAMETERS · SAP GL (cost)     │
└───────────────────────────┬─────────────────────────────────────────────┘
                            │
                            ▼
                  ┌──────────────────┐
                  │  PHASE 1         │
                  │  Change          │
                  │  Detection       │  CDC on CHANNEL_HIERARCHY_MASTER
                  │  (daily)         │  → emits ChangeEvent
                  └────────┬─────────┘
                           │
              ┌────────────┴─────────────┐
              │                          │
              ▼                          ▼
    ┌──────────────────┐      ┌──────────────────────┐
    │  PHASE 2         │      │  PHASE 3              │
    │  Baseline        │      │  Forward Simulation   │
    │  Computation     │      │  (pre-launch)         │
    │                  │      │                       │
    │  52-week P&L     │      │  Ramp curve model     │
    │  snapshot with   │      │  3 scenarios          │
    │  seasonal index  │      │  (cons/base/opt)      │
    └────────┬─────────┘      └──────────┬────────────┘
             │                           │
             └────────────┬──────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │  PHASE: APPROVAL      │
              │  Gate 1 — Business    │  Signs off on P&L impact
              │  Gate 2 — Engineering │  Signs off on pipeline impact
              └───────────┬───────────┘
                          │ Both approved
                          ▼
                  [change goes live]
                          │
              ┌───────────┴───────────┐
              │  PHASE 4              │
              │  Actual Impact        │
              │  Measurement          │
              │                       │
              │  Difference-in-Diff   │
              │  Attribution by       │
              │  volume / rate / cost │
              └───────────┬───────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │  PHASE 5              │
              │  Sim vs Actual        │
              │  Reconciliation       │
              │                       │
              │  Accuracy registry    │
              │  Calibrate future     │
              │  simulations          │
              └───────────┬───────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │  AI LAYER             │
              │  Narrative (Haiku)    │
              │  Deviation agent      │
              │  (Opus if >20% miss)  │
              └───────────┬───────────┘
                          │
          ┌───────────────┼────────────────┐
          ▼               ▼                ▼
   Analyst Self-Serve  Exec Dashboard   API
   (Scenario UI)       (BQ / Looker)    (Forecasting models)
```

---

## 4. Phase 1 — Change Detection

**Trigger:** Daily scheduled job compares today's `CHANNEL_HIERARCHY_MASTER` to yesterday's snapshot.

**Detected change types:**

| Signal | Detection Logic |
|--------|----------------|
| New store | `store_id` present in today, absent in yesterday |
| Store retired | `store_id` absent in today, present in yesterday |
| Reclassification | `store_id` present in both, but `channel_cd`, `fulfillment_type`, or `commission_rate` changed |
| Channel split | New `channel_cd` values appear that didn't exist before |

**Output:** A `ChangeEvent` record written to `PNL_IMPACT_EVENT` table (schema in §11).

**Key design decision:** Change detection runs even on weekends. If a hierarchy change is applied on a Friday,
the simulation and Gate 1 notification fires Saturday — analyst reviews Monday before the week's data loads.

---

## 5. Phase 2 — Baseline Computation

**What:** Freeze an immutable 52-week P&L snapshot at T-0 (the last full day before the change effective date).

**Why 52 weeks, not 90 days:**
- eCommerce holiday (Q4) weeks are 2–4× non-holiday volume
- A 90-day window containing Black Friday vs one that doesn't gives incomparable baselines
- 52 weeks captures every seasonal pattern the store has ever exhibited

**Seasonal index:** For each store, compute each calendar week's share of its annual GMV/CM.
This index is used in Phase 3 to make simulations seasonally aware — a new store launching in October
will be projected using October's seasonal multiplier, not a flat average.

**For new stores (no 52-week history):** Use peer stores (similar channel, fulfillment type, geography)
as the baseline proxy. Inherit peer store seasonal index.

---

## 6. Phase 3 — Forward Simulation (Pre-launch)

Two simulation modes depending on change type:

### Mode A: New Store
No order history exists. Project from peer store baseline × ramp curve × seasonal index.

```
Projected GMV (week N) = Annual_peer_baseline × seasonal_index(week N) × ramp_factor(week N)

Ramp curve: new stores don't instantly reach peer volume.
  Week 1:  ~10% of peer volume
  Week 4:  ~40%
  Week 8:  ~70%
  Week 13: ~85%
  (analyst-adjustable via UI)
```

Three scenarios output:

| Scenario | Ramp multiplier | Use |
|----------|----------------|-----|
| Conservative | 0.7× base ramp | Downside / risk case |
| Base | Peer historical ramp | Planning case |
| Optimistic | 1.3× base ramp (capped at 100%) | Upside case |

### Mode B: Reclassification
Order history exists. Apply new commission rate / cost structure to known volume.

```
CM delta (week N) = order_volume(week N) × (new_commission_rate − old_commission_rate) × AOV
                  + order_volume(week N) × (old_cost_per_order − new_cost_per_order)
```

No ramp curve needed — volume is known, rate change is instantaneous.

**Simulation output:** Weekly P&L projection for 13 weeks × 3 scenarios.
Annualised CM impact, break-even week (for new stores), confidence range.

---

## 7. Phase 4 — Actual Impact Measurement (Post-launch)

**Problem:** After launch, raw P&L comparison is misleading.
If GMV is up 10% post-launch, is that because of the new store — or because it's peak season?

**Solution: Difference-in-Differences (DiD)**

```
Causal P&L impact = (Treatment_post − Treatment_pre) − (Control_post − Control_pre)

Treatment: the changed store(s)
Control:   similar stores that did NOT change in the same period (peer stores)
Pre:       T-52w to T-0 (same as baseline)
Post:      T+1w to T+13w
```

DiD strips out market-wide trends (seasonality, macro, concurrent promotions)
and isolates the signal attributable to the hierarchy change itself.

**Attribution decomposition** breaks the total CM delta into root causes:

```
Total CM delta
  = Volume effect         (did orders increase/decrease?)
  + Commission effect     (did take rate change move revenue?)
  + Refund rate effect    (did return rate change?)
  + Cost effect           (did per-order cost change?)
  + Residual              (interaction effects)
```

---

## 8. Phase 5 — Simulation vs Actual Reconciliation

After 13 weeks post-launch, compare what the simulation predicted to what actually happened.

**Accuracy metrics tracked:**

| Metric | Definition |
|--------|-----------|
| MAPE | Mean absolute % error: `|actual_CM − sim_base_CM| / sim_base_CM` |
| Direction accuracy | Did simulation correctly predict positive vs negative CM impact? |
| Within bounds | Was actual CM between conservative and optimistic scenarios? |
| Ramp accuracy | How close was the actual weekly ramp to the projected ramp? |

**Output:** Written to `PNL_SIMULATION_ACCURACY` table (schema in §11).
Aggregated accuracy by `change_type` feeds back into future simulations —
ramp curve priors and refund rate assumptions are calibrated from real outcomes.

---

## 9. Two-Gate Approval Workflow

No hierarchy change goes live without both gates cleared. Neither can be bypassed.

```
Simulation complete
       │
       ▼
┌──────────────────────────────────────────────────────┐
│  GATE 1 — BUSINESS APPROVAL                          │
│  Who: Finance stakeholder / Business owner           │
│  Question: Is this P&L impact acceptable?            │
│                                                      │
│  Sees:                                               │
│  · 3-scenario P&L waterfall (13-week projection)    │
│  · Annualised CM impact ($)                         │
│  · Key assumptions (ramp, commission rate, refunds) │
│  · AI-generated plain-English narrative             │
└─────────────────┬────────────────────────────────────┘
                  │ APPROVED
                  ▼
┌──────────────────────────────────────────────────────┐
│  GATE 2 — ENGINEERING APPROVAL                       │
│  Who: Eng lead / Data engineering owner              │
│  Question: Does this break anything in the           │
│            eComm P&L pipeline or data model?         │
│                                                      │
│  Auto-generated impact report shows:                 │
│  · Which BQ tables reference this store_id           │
│  · How many historical rows reclassify               │
│    (scope of data change if existing orders          │
│     are re-bucketed into the new channel)            │
│  · Which Airflow DAGs load this store's data         │
│    (checked against ETL_LOAD_PARAMETERS)             │
│  · Missing pipeline configs for new store_id         │
│    (if store_id not in load configs → data           │
│     will be silently dropped post-launch)            │
└─────────────────┬────────────────────────────────────┘
                  │ APPROVED
                  ▼
         deployment_unblocked = TRUE
         Concord / CI pipeline gate passes
         Hierarchy change goes live
```

**Approval mechanics:**
- Both approvers receive email with Approve / Reject links
- Gate 2 email only fires after Gate 1 is approved
- Approval click → updates `PNL_IMPACT_EVENT` with approver ID + timestamp
- Deployment pipeline checks `deployment_unblocked` flag as a hard gate before activating change

**What the Engineering gate catches that's easy to miss:**
A new `store_id` added to the hierarchy but absent from `ETL_LOAD_PARAMETERS`
will silently drop all its orders — they exist in OMS2 but never flow into BQ.
The eng impact report surfaces this before deployment, not after.

---

## 10. AI Layer

AI activates at two points — cost-optimised by volume.

### Narrative generation (every event — Claude Haiku)

After simulation completes, Haiku auto-generates a 3-paragraph plain-English summary:
1. What changed and when
2. Projected P&L impact (range across scenarios)
3. Key assumptions and risks

This goes into the Gate 1 approval email so the business approver sees narrative, not raw numbers.

### Deviation analysis (triggered — Claude Opus)

Fires only when `|actual_CM − sim_base_CM| / sim_base_CM > 20%` — a significant miss.

Opus traces the deviation to its root cause by querying order-level data:
- Was the ramp curve wrong (volume came in different from peer proxy)?
- Did refund rates spike unexpectedly?
- Was there a concurrent event (promotion, competitor, GCP outage) that the simulation didn't model?
- Does this suggest a systematic error in how we chose peer stores?

Output: structured deviation report + updated simulation assumptions for next change of same type.

### Model routing

| Condition | Model | Trigger |
|-----------|-------|---------|
| Every change event (narrative) | Claude Haiku | Simulation complete |
| >20% sim vs actual miss | Claude Opus | Phase 5 accuracy check |
| Analyst asks "why?" question | Claude Haiku / Opus depending on complexity | On-demand |

Internal Walmart knowledge (Confluence, internal Stack Overflow via `content_search`) is queried before
any external source — internal pipeline context is more relevant than public documentation.

---

## 11. Data Model & Schemas

### `PNL_IMPACT_EVENT` — master registry

```sql
CREATE TABLE US_FIN_ECOMM_DL_TABLES.PNL_IMPACT_EVENT (

    -- Identity
    event_id                STRING    NOT NULL,   -- UUID, primary key
    change_type             STRING,               -- NEW_STORE | RECLASSIFIED | RETIRED | SPLIT
    store_id                STRING,
    old_channel_cd          STRING,               -- NULL for NEW_STORE
    new_channel_cd          STRING,
    old_commission_rate     FLOAT64,
    new_commission_rate     FLOAT64,
    effective_dt            DATE,                 -- when change goes live
    detected_dt             DATE,                 -- when CDC detected the change

    -- Phase status timestamps
    baseline_frozen_ts      TIMESTAMP,
    simulation_run_ts       TIMESTAMP,
    impact_measured_ts      TIMESTAMP,            -- populated post-launch
    reconciliation_ts       TIMESTAMP,            -- populated 13w post-launch

    -- Business approval (Gate 1)
    biz_approved            BOOL,
    biz_approver_id         STRING,
    biz_approval_ts         TIMESTAMP,
    biz_approval_notes      STRING,
    biz_sim_scenario        STRING,               -- which scenario approved ('base'|'conservative')

    -- Engineering approval (Gate 2)
    eng_approved            BOOL,
    eng_approver_id         STRING,
    eng_approval_ts         TIMESTAMP,
    eng_approval_notes      STRING,
    eng_pipeline_risk_level STRING,               -- LOW | MEDIUM | HIGH

    -- Combined gate
    deployment_unblocked    BOOL,                 -- TRUE only when both gates approved

    -- P&L summary (for dashboard queries — avoids joining all child tables)
    sim_conservative_cm_13w FLOAT64,
    sim_base_cm_13w         FLOAT64,
    sim_optimistic_cm_13w   FLOAT64,
    actual_cm_13w           FLOAT64,              -- populated post-launch
    causal_cm_impact        FLOAT64,              -- DiD-adjusted actual impact
    sim_accuracy_mape       FLOAT64,              -- populated post-reconciliation

    -- Metadata
    business_owner_email    STRING,
    eng_owner_email         STRING,
    affected_order_vol_l52w INT64,                -- historical order volume (proxy for change magnitude)
    peer_store_ids          ARRAY<STRING>         -- stores used as baseline proxy

) PRIMARY KEY (event_id) NOT ENFORCED
```

---

### `PNL_IMPACT_BASELINE` — 52-week frozen snapshot

```sql
CREATE TABLE US_FIN_ECOMM_DL_TABLES.PNL_IMPACT_BASELINE (

    event_id                STRING    NOT NULL,   -- FK to PNL_IMPACT_EVENT
    store_id                STRING,
    channel_cd              STRING,
    week_dt                 DATE,                 -- Monday of each week
    week_num                INT64,                -- 0–51 (0 = 52 weeks before effective_dt)

    -- Revenue
    gmv                     FLOAT64,
    net_revenue             FLOAT64,
    commission              FLOAT64,

    -- Refunds (from WM_SALES_ORDER_INV_CHRG_DTL event_nm dimension)
    oms_refund              FLOAT64,
    rap_refund              FLOAT64,
    cb_refund               FLOAT64,
    adjustment              FLOAT64,
    total_refund            FLOAT64,              -- sum of above
    refund_rate             FLOAT64,              -- total_refund / gmv

    -- Volume
    order_count             INT64,
    aov                     FLOAT64,              -- average order value

    -- Cost (source TBD ⬜)
    fulfillment_cost        FLOAT64,

    -- Derived
    contribution_margin     FLOAT64,
    cm_per_order            FLOAT64,

    -- Seasonal index for this store × calendar week
    gmv_seasonal_idx        FLOAT64,              -- this week's share of annual GMV
    cm_seasonal_idx         FLOAT64,
    volume_seasonal_idx     FLOAT64,

    -- Annual totals (for projection math)
    annual_gmv_baseline     FLOAT64,
    annual_cm_baseline      FLOAT64,

    baseline_frozen_ts      TIMESTAMP,
    is_peer_proxy           BOOL                  -- TRUE if this store used as peer for a new store

) PARTITION BY DATE_TRUNC(week_dt, MONTH)
  CLUSTER BY event_id, store_id
```

---

### `PNL_SIMULATION_RESULT` — forward projection

```sql
CREATE TABLE US_FIN_ECOMM_DL_TABLES.PNL_SIMULATION_RESULT (

    event_id                STRING    NOT NULL,
    scenario                STRING,               -- 'conservative' | 'base' | 'optimistic'
    week_num                INT64,                -- 1–13 (post-launch)
    projected_week_dt       DATE,

    -- Projected P&L
    proj_gmv                FLOAT64,
    proj_commission         FLOAT64,
    proj_refund             FLOAT64,              -- uses refund_rate_assumption
    proj_fulfillment_cost   FLOAT64,
    proj_contribution_margin FLOAT64,
    proj_order_count        INT64,

    -- Assumptions used (for auditability)
    ramp_factor             FLOAT64,              -- % of peer volume this week
    seasonal_idx_applied    FLOAT64,
    commission_rate_used    FLOAT64,
    refund_rate_assumption  FLOAT64,
    cost_per_order_assumption FLOAT64,

    simulation_run_ts       TIMESTAMP

) PARTITION BY DATE_TRUNC(projected_week_dt, MONTH)
  CLUSTER BY event_id, scenario
```

---

### `PNL_ACTUAL_IMPACT` — post-launch measurement

```sql
CREATE TABLE US_FIN_ECOMM_DL_TABLES.PNL_ACTUAL_IMPACT (

    event_id                STRING    NOT NULL,
    week_num                INT64,                -- 1–13 post-launch
    actual_week_dt          DATE,

    -- Actuals (treatment store)
    actual_gmv              FLOAT64,
    actual_commission       FLOAT64,
    actual_refund           FLOAT64,
    actual_refund_rate      FLOAT64,
    actual_fulfillment_cost FLOAT64,
    actual_cm               FLOAT64,
    actual_order_count      INT64,

    -- Control group (peer stores — same weeks)
    control_gmv             FLOAT64,
    control_cm              FLOAT64,

    -- Difference-in-Differences
    treatment_delta_cm      FLOAT64,              -- actual_CM − baseline_CM (treatment)
    control_delta_cm        FLOAT64,              -- control_post_CM − control_pre_CM
    causal_cm_impact        FLOAT64,              -- treatment_delta − control_delta

    -- Attribution decomposition
    volume_effect           FLOAT64,
    commission_effect       FLOAT64,
    refund_rate_effect      FLOAT64,
    cost_effect             FLOAT64,
    residual_effect         FLOAT64,              -- interaction / unexplained

    measured_ts             TIMESTAMP

) PARTITION BY DATE_TRUNC(actual_week_dt, MONTH)
  CLUSTER BY event_id
```

---

### `PNL_SIMULATION_ACCURACY` — reconciliation & calibration

```sql
CREATE TABLE US_FIN_ECOMM_DL_TABLES.PNL_SIMULATION_ACCURACY (

    event_id                STRING    NOT NULL,
    change_type             STRING,

    -- Simulation predictions (base case)
    sim_base_cm_13w         FLOAT64,
    sim_conservative_cm_13w FLOAT64,
    sim_optimistic_cm_13w   FLOAT64,

    -- Actuals
    actual_cm_13w           FLOAT64,
    causal_cm_13w           FLOAT64,              -- DiD-adjusted

    -- Accuracy metrics
    mape                    FLOAT64,              -- |actual − sim_base| / |sim_base|
    direction_correct       BOOL,                 -- sign of impact predicted correctly?
    actual_within_bounds    BOOL,                 -- actual between conservative and optimistic?

    -- Ramp accuracy (new store only)
    ramp_actual_wk1         FLOAT64,              -- actual ramp achieved each week
    ramp_actual_wk4         FLOAT64,
    ramp_actual_wk8         FLOAT64,
    ramp_actual_wk13        FLOAT64,
    ramp_assumed_wk1        FLOAT64,              -- what simulation assumed
    ramp_assumed_wk4        FLOAT64,
    ramp_assumed_wk8        FLOAT64,
    ramp_assumed_wk13       FLOAT64,

    -- Refund rate accuracy
    refund_rate_actual      FLOAT64,
    refund_rate_assumed     FLOAT64,

    -- Deviation analysis (Opus-generated, populated only if MAPE > 20%)
    deviation_root_causes   ARRAY<STRING>,
    model_adjustment_recommended BOOL,

    reconciled_ts           TIMESTAMP

) CLUSTER BY change_type
```

---

### `CHANNEL_HIERARCHY_SNAPSHOT` — daily CDC snapshot (new)

```sql
-- Daily point-in-time snapshot of CHANNEL_HIERARCHY_MASTER for CDC diffing
CREATE TABLE US_FIN_ECOMM_DL_TABLES.CHANNEL_HIERARCHY_SNAPSHOT (

    snapshot_dt             DATE,
    store_id                STRING,
    store_nm                STRING,
    channel_cd              STRING,
    fulfillment_type        STRING,
    commission_rate         FLOAT64,
    effective_dt            DATE,
    record_hash             STRING    -- SHA256 of (channel_cd, fulfillment_type, commission_rate)

) PARTITION BY snapshot_dt
  CLUSTER BY store_id
-- Retained 90 days (change detection only needs yesterday vs today)
```

---

## 12. Output Surfaces

### Analyst self-serve (Scenario UI)

```
┌──────────────────────────────────────────────────────────────┐
│ INPUT PANEL                                                   │
│ Change type: [NEW_STORE ▼]   Store ID: [_______]             │
│ Effective date: [2026-09-01]                                  │
│ Volume ramp: ████░░░░░░░░░  (adjustable slider)              │
│ Commission rate: 12% → 15%   Refund rate assumption: [4.2%]  │
│ Peer stores: [9101, 9202, 9303] (auto-suggested, editable)   │
└──────────────────────────────────────────────────────────────┘

OUTPUT
  · 13-week P&L waterfall chart (3 scenarios overlaid)
  · Break-even week indicator (new stores)
  · Annualised CM impact: $X.XM (cons) — $X.XM (opt)
  · AI narrative summary (plain English, 3 paragraphs)
  · [Submit for Business Approval] button
```

### Exec dashboard (BQ → Looker Studio)

```
Page 1: Active Changes
  Table: all open events with phase status and traffic-light indicator
  (simulation vs actual trajectory for post-launch events)

Page 2: P&L Impact Summary
  KPIs: Total simulated CM impact ($), Total measured causal CM YTD ($)
        Simulation accuracy score (MAPE %), # events auto-within-bounds

Page 3: Drill-down per event
  · Waterfall: attribution breakdown (volume / commission / refund / cost)
  · Simulation vs actual overlay chart (13 weeks)
  · AI narrative + anomaly flag (if >20% deviation)
  · Approval status (Gate 1 ✅ / Gate 2 ✅ / Pending)
```

### Downstream API

```
GET  /pnl-impact/{event_id}
     → SimulationResult + ActualImpact + AccuracyRecord

GET  /pnl-impact/store/{store_id}/history
     → All change events and outcomes for a store

POST /pnl-impact/simulate
     Body: {change_type, store_id, effective_dt, assumptions}
     → SimulationResult (synchronous)

GET  /pnl-impact/accuracy?change_type=NEW_STORE
     → Aggregated accuracy metrics (calibration feed for forecasting models)
```

---

## 13. Implementation Phases

| Phase | What | Deliverable |
|-------|------|-------------|
| **P0** | CDC detection on existing `CHANNEL_HIERARCHY_MASTER` | `CHANNEL_HIERARCHY_SNAPSHOT` table + daily diff job → ChangeEvent |
| **P1** | 52-week baseline with seasonal index | `PNL_IMPACT_BASELINE` schema + freeze job on change detect |
| **P2** | Reclassification simulation | `PNL_SIMULATION_RESULT` schema + Mode B (commission/cost delta) |
| **P3** | New store simulation | Mode A (ramp curve × peer proxy × seasonal index), 3 scenarios |
| **P4** | Two-gate approval workflow | Email notifications, approval tracking in `PNL_IMPACT_EVENT`, deployment gate check |
| **P5** | Actual impact measurement (DiD) | `PNL_ACTUAL_IMPACT` schema + weekly attribution computation |
| **P6** | Haiku narrative generation | Plain-English summaries for Gate 1 email and analyst UI |
| **P7** | Analyst self-serve UI | Scenario input panel + waterfall chart |
| **P8** | Exec dashboard | Looker Studio / BQ dashboard (3 pages above) |
| **P9** | Sim vs actual reconciliation | `PNL_SIMULATION_ACCURACY` schema + calibration of ramp / refund priors |
| **P10** | Opus deviation analysis | Root-cause agent for >20% simulation misses |

**MVP (P0–P6):** Change detection → baseline → simulation → approval gates → measurement → AI narrative
**Full system (P0–P10):** All above + self-serve UI + exec dashboard + calibration loop

---

## 14. Open Questions

| # | Question | Status |
|---|----------|--------|
| 1 | Does `CHANNEL_HIERARCHY_MASTER` exist in BQ? | ✅ Yes |
| 2 | **Fulfillment cost source — which BQ table or API?** | ⬜ TBD |
| 3 | Anaplan integration needed? | ✅ No |
| 4 | Two-gate approval: Business + Engineering | ✅ Confirmed |
| 5 | Baseline window | ✅ 52-week with seasonal decomposition |

---

*Data sources: GCP BigQuery · Airflow (FDL_CoreFinance) · OMS2 · Plutus/Kafka · SAP GL*
*AI: Claude Haiku 4 (narrative) · Claude Opus 4 (deviation analysis, triggered only)*
