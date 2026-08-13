# eCommerce P&L Impact Analysis & Simulation Architecture
### Channel Hierarchy Changes · New Store Onboarding · Walmart Finance Engineering

> **Core question this system answers:**
> *"When a channel hierarchy changes or a new store goes live — what is the P&L impact,
> and how does it compare to what we predicted?"*

> **Assumptions (correct if wrong):**
> - Scope: Walmart.com eCommerce P&L (FDL_CoreFinance pipelines, OMS2, Plutus, BQ)
> - P&L lines: GMV, commissions, refunds (OMS + RAP), adjustments, contribution margin
> - Trigger: channel hierarchy change or new store_id in source data
> - Workflow: auto-detect change → simulate impact → analyst reviews → measure actuals post-launch
> - Users: Finance analysts (self-serve), Exec dashboards, downstream forecasting models

---

## Table of Contents

1. [What is a Channel Hierarchy Change?](#1-what-is-a-channel-hierarchy-change)
2. [P&L Components in Scope](#2-pl-components-in-scope)
3. [Overall Architecture](#3-overall-architecture)
4. [Phase 1: Change Detection](#4-phase-1-change-detection)
5. [Phase 2: Baseline Computation](#5-phase-2-baseline-computation)
6. [Phase 3: Forward Simulation (Pre-launch)](#6-phase-3-forward-simulation-pre-launch)
7. [Phase 4: Actual Impact Measurement (Post-launch)](#7-phase-4-actual-impact-measurement-post-launch)
8. [Phase 5: Simulation vs Actual Reconciliation](#8-phase-5-simulation-vs-actual-reconciliation)
9. [Data Model](#9-data-model)
10. [AI Layer — LLM-Assisted Analysis](#10-ai-layer--llm-assisted-analysis)
11. [Output Layer — Analyst & Exec Surfaces](#11-output-layer--analyst--exec-surfaces)
12. [Implementation Roadmap](#12-implementation-roadmap)

---

## 1. What is a Channel Hierarchy Change?

In Walmart eCommerce, orders flow through a **channel hierarchy**:

```
Business Unit (Walmart.com)
  └── Channel (Marketplace / First-Party / SamsClub.com)
        └── Fulfillment Type (1P / 3P / WFS / Club Pickup)
              └── Store / Facility Node (store_id, club_nbr, FC_id)
                    └── Seller / Category
```

A **hierarchy change** is any mutation to this tree:

| Change type | Example | P&L impact |
|-------------|---------|-----------|
| **New store node added** | New fulfillment center goes live (store_id=9876) | Orders shift from existing nodes → new cost/revenue attribution |
| **Store reclassified** | FC re-typed from 1P→WFS | Commission rate changes, fulfillment cost bucket changes |
| **Channel split** | Marketplace split into "Marketplace Ads" + "Marketplace Base" | Revenue lines bifurcate; historical comparisons break |
| **Store merged / retired** | Two regional FCs consolidated | Orders reroute; P&L appears to "move" between nodes |
| **New seller onboarded** | New 3P seller on marketplace | Commission + refund rate baseline doesn't exist yet |

Each of these creates **two problems**:
1. **Forward:** What will this do to P&L? (simulation before launch)
2. **Backward:** After launch, how much of the P&L change is actually due to this, vs external factors?

---

## 2. P&L Components in Scope

Based on existing FDL_CoreFinance pipeline tables:

```
GROSS MERCHANDISE VALUE (GMV)
  └── WM_SALES_ORDER_INV_CHRG_DTL  (charge detail per order line)
  └── WM_SALES_ORDER_INV_TNDR_DTL  (tender / payment detail)

NET REVENUE
  = GMV
  - OMS_REFUND   (refunds initiated via Order Management System)
  - RAP_REFUND   (refunds via Returns & Adjustments Platform — physical return)
  - CB_REFUND    (chargeback refunds)
  - ADJUSTMENT   (price corrections, post-order adjustments)

COMMISSION / TAKE RATE  (Marketplace only)
  = GMV × commission_rate(seller, category, channel)

FULFILLMENT COST
  = shipping_cost + warehouse_handling + last_mile_cost
  (from SAP GL posting via sub-ledger API)

CONTRIBUTION MARGIN (CM)
  = Net Revenue - Fulfillment Cost - Seller Payouts - Promotions

UNIT ECONOMICS (per order)
  = CM / order_count
```

---

## 3. Overall Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  DATA SOURCES                                                                 │
│  OMS2 (orders) · Plutus/Kafka (real-time events) · SAP (GL/costs)            │
│  ETL_LOAD_PARAMETERS (BQ) · WM_SALES_ORDER_INV_CHRG_DTL (BQ/Hudi)           │
│  Channel Hierarchy Master (BQ reference table)                               │
└─────────────────────────┬────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  PHASE 1: CHANGE DETECTION                                                    │
│  CDC on Channel Hierarchy Master table (BQ audit log or Hudi timeline)       │
│  Detects: new store_id, reclassification, channel split/merge                │
│  Emits: ChangeEvent {change_type, affected_nodes, effective_date}            │
└─────────────────────────┬────────────────────────────────────────────────────┘
                          │
             ┌────────────┴────────────┐
             │                         │
             ▼                         ▼
┌─────────────────────┐   ┌──────────────────────────────────────────────────┐
│  PHASE 2:           │   │  PHASE 3: FORWARD SIMULATION (pre-launch)         │
│  BASELINE           │   │                                                   │
│  COMPUTATION        │   │  Scenario Engine:                                 │
│                     │   │  · Transfer baseline P&L to new node structure    │
│  Freeze P&L         │   │  · Apply commission rate changes                  │
│  snapshot at T-0    │   │  · Apply ramp curve (new store ≠ instant volume)  │
│  before change      │   │  · Monte Carlo: low / base / high case            │
│  goes live          │   │  · Output: simulated P&L delta by line item        │
└─────────────────────┘   └──────────────────────────────────────────────────┘
             │                         │
             └────────────┬────────────┘
                          │  [change goes live]
                          ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  PHASE 4: ACTUAL IMPACT MEASUREMENT (post-launch)                            │
│  · Compare post-launch P&L to frozen T-0 baseline                            │
│  · Isolate channel change signal from external noise (seasonality, GMV trend)│
│  · Difference-in-Differences: treatment node vs control nodes                │
│  · Attribution: how much of delta = channel change vs market movement?        │
└─────────────────────────┬────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  PHASE 5: SIMULATION vs ACTUAL RECONCILIATION                                 │
│  · Did simulation predict the right direction and magnitude?                  │
│  · Improve simulation assumptions for next change                             │
│  · Store delta in accuracy registry → calibrate future simulations            │
└─────────────────────────┬────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  AI LAYER (Phase 2–5)                                                         │
│  · LLM generates natural language narrative of P&L impact                    │
│  · Anomaly detector flags unexpected post-launch deviations                  │
│  · "Why did this happen?" agent traces deviation to root data change          │
└──────────────────────────────────────────────────────────────────────────────┘
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
   Analyst Self-Serve  Exec Dashboard  Downstream API
   (Scenario UI)       (BQ/Looker)     (Forecasting models)
```

---

## 4. Phase 1: Change Detection

The system needs to know *when* a hierarchy change happens — before a human tells it.

### Source of truth: Channel Hierarchy Master

```sql
-- BQ table: US_FIN_ECOMM_DL_TABLES.CHANNEL_HIERARCHY_MASTER
-- Schema:
CREATE TABLE IF NOT EXISTS CHANNEL_HIERARCHY_MASTER (
    store_id          STRING,
    store_nm          STRING,
    channel_cd        STRING,   -- 'MARKETPLACE' | '1P' | 'WFS' | 'CLUB'
    channel_nm        STRING,
    fulfillment_type  STRING,
    business_unit     STRING,
    commission_rate   FLOAT64,
    effective_dt      DATE,
    expiry_dt         DATE,     -- NULL = currently active
    record_hash       STRING,   -- SHA256 of key fields for CDC
    created_ts        TIMESTAMP,
    updated_ts        TIMESTAMP
)
```

### CDC (Change Data Capture) watcher

```python
# change_detection/hierarchy_watcher.py
# Runs as daily Airflow task or triggered by BQ audit log Pub/Sub

def detect_hierarchy_changes(bq_client, run_date: date) -> List[ChangeEvent]:
    """
    Compare today's CHANNEL_HIERARCHY_MASTER to yesterday's snapshot.
    Detect: new rows (new store), updated rows (reclassification), deleted rows (retirement).
    """
    query = """
    WITH today AS (
        SELECT store_id, channel_cd, fulfillment_type, commission_rate,
               record_hash, effective_dt
        FROM US_FIN_ECOMM_DL_TABLES.CHANNEL_HIERARCHY_MASTER
        WHERE expiry_dt IS NULL  -- active records
    ),
    yesterday AS (
        SELECT store_id, channel_cd, fulfillment_type, commission_rate,
               record_hash
        FROM US_FIN_ECOMM_DL_TABLES.CHANNEL_HIERARCHY_MASTER_SNAPSHOT
        WHERE snapshot_dt = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    )
    SELECT
        COALESCE(t.store_id, y.store_id)  AS store_id,
        CASE
            WHEN y.store_id IS NULL         THEN 'NEW_STORE'
            WHEN t.store_id IS NULL         THEN 'STORE_RETIRED'
            WHEN t.record_hash != y.record_hash THEN 'RECLASSIFIED'
        END AS change_type,
        y.channel_cd    AS old_channel,
        t.channel_cd    AS new_channel,
        y.commission_rate AS old_commission_rate,
        t.commission_rate AS new_commission_rate,
        CURRENT_DATE() AS detected_dt
    FROM today t
    FULL OUTER JOIN yesterday y USING (store_id)
    WHERE t.record_hash != y.record_hash
       OR y.store_id IS NULL
       OR t.store_id IS NULL
    """
    changes = bq_client.query(query).result()
    return [ChangeEvent.from_row(row) for row in changes]
```

### Change event schema

```python
@dataclass
class ChangeEvent:
    event_id:           str       # UUID
    change_type:        str       # NEW_STORE | RECLASSIFIED | STORE_RETIRED | CHANNEL_SPLIT
    store_id:           str
    old_channel:        str       # None for NEW_STORE
    new_channel:        str
    old_commission_rate: float    # None for NEW_STORE
    new_commission_rate: float
    effective_dt:       date
    detected_dt:        date
    affected_order_volume_l90d: int   # lookback order count (0 for new store)
    similar_past_events: List[str]    # event_ids of past similar changes (for simulation seeding)
```

---

## 5. Phase 2: Baseline Computation

Before any simulation or measurement, freeze the P&L state at **T-0** (day before change effective date).

```sql
-- baseline/freeze_baseline.sql
-- Creates immutable snapshot of P&L by store/channel for the 90 days before change

CREATE OR REPLACE TABLE US_FIN_ECOMM_DL_TABLES.PNL_IMPACT_BASELINE
AS
SELECT
    event_id,                        -- links back to ChangeEvent
    store_id,
    channel_cd,
    DATE_TRUNC(order_dt, WEEK)       AS week_dt,

    -- Revenue lines
    SUM(gmv_amt)                     AS gmv,
    SUM(net_sales_amt)               AS net_revenue,
    SUM(commission_amt)              AS commission,

    -- Refund lines (from WM_SALES_ORDER_INV_CHRG_DTL)
    SUM(CASE WHEN event_nm = 'OMS_REFUND'  THEN chrg_amt ELSE 0 END) AS oms_refund,
    SUM(CASE WHEN event_nm = 'RAP_REFUND'  THEN chrg_amt ELSE 0 END) AS rap_refund,
    SUM(CASE WHEN event_nm = 'CB_REFUND'   THEN chrg_amt ELSE 0 END) AS cb_refund,
    SUM(CASE WHEN event_nm = 'ADJUSTMENT'  THEN chrg_amt ELSE 0 END) AS adjustment,

    -- Unit metrics
    COUNT(DISTINCT sales_order_num)  AS order_count,
    SUM(gmv_amt) / NULLIF(COUNT(DISTINCT sales_order_num), 0) AS aov,

    -- Cost lines (from SAP GL)
    SUM(fulfillment_cost_amt)        AS fulfillment_cost,

    -- Derived
    SUM(net_sales_amt) - SUM(fulfillment_cost_amt) AS contribution_margin,

    CURRENT_TIMESTAMP()              AS baseline_frozen_ts

FROM US_FIN_ECOMM_DL_TABLES.WM_SALES_ORDER_INV_CHRG_DTL chrg
JOIN US_FIN_ECOMM_DL_TABLES.CHANNEL_HIERARCHY_MASTER hier
    USING (store_id)
WHERE order_dt BETWEEN DATE_SUB(@effective_dt, INTERVAL 90 DAY) AND @effective_dt
  AND store_id = @store_id

GROUP BY 1, 2, 3, 4
```

**Why 90-day baseline?**
- Captures full seasonality cycle (13 weeks)
- Enough volume for statistical significance even for low-traffic stores
- Aligns with Anaplan / MFP planning cycle used by Sam's Club finance

---

## 6. Phase 3: Forward Simulation (Pre-launch)

The simulation answers: *"If this store goes live / this channel changes, what will the P&L look like?"*

### Simulation engine inputs

```python
@dataclass
class SimulationInputs:
    change_event:       ChangeEvent
    baseline:           PnLBaseline         # from Phase 2

    # Scenario assumptions (analyst-configurable)
    volume_ramp_curve:  List[float]         # e.g. [0.1, 0.3, 0.6, 0.85, 1.0] over 5 weeks
    commission_delta:   float               # change in take rate (e.g. +0.005 = +0.5%)
    refund_rate_assumption: float           # expected refund rate for new store (default: peer avg)
    fulfillment_cost_assumption: float      # $/order for new node

    # Comparable stores (for new store — no history exists)
    peer_stores:        List[str]           # similar store_ids to use as proxy baseline
```

### Ramp curve model (new store)

A new store does not instantly reach full volume. Model a ramp:

```python
def simulate_new_store_pnl(inputs: SimulationInputs, weeks: int = 13) -> SimulationResult:
    """
    Project weekly P&L for a new store based on peer store baseline + ramp curve.
    Run 3 scenarios: base / optimistic / conservative.
    """
    peer_baseline = _get_peer_weekly_avg(inputs.peer_stores)

    scenarios = {
        'conservative': [r * 0.7 for r in inputs.volume_ramp_curve],
        'base':         inputs.volume_ramp_curve,
        'optimistic':   [min(r * 1.3, 1.0) for r in inputs.volume_ramp_curve],
    }

    results = {}
    for scenario_name, ramp in scenarios.items():
        weekly_pnl = []
        for week_idx, ramp_factor in enumerate(ramp[:weeks]):
            projected = PnLWeek(
                week=week_idx + 1,
                gmv=peer_baseline.gmv * ramp_factor,
                commission=peer_baseline.gmv * ramp_factor * inputs.commission_delta,
                refund=peer_baseline.gmv * ramp_factor * inputs.refund_rate_assumption,
                fulfillment_cost=peer_baseline.order_count * ramp_factor
                                 * inputs.fulfillment_cost_assumption,
            )
            projected.contribution_margin = (
                projected.gmv
                - projected.refund
                - projected.fulfillment_cost
            )
            weekly_pnl.append(projected)
        results[scenario_name] = weekly_pnl

    return SimulationResult(
        event_id=inputs.change_event.event_id,
        scenarios=results,
        cumulative_13w_cm={k: sum(w.contribution_margin for w in v)
                           for k, v in results.items()},
        break_even_week=_find_breakeven(results['base']),
    )
```

### Reclassification simulation

For existing stores changing channel/commission:

```python
def simulate_reclassification(inputs: SimulationInputs, weeks: int = 13) -> SimulationResult:
    """
    Apply new commission rate and cost structure to existing baseline volume.
    No ramp curve needed — volume is known.
    """
    baseline = inputs.baseline
    delta_commission_rate = inputs.commission_delta

    weekly_pnl = []
    for week in baseline.weekly_data:
        new_commission = week.gmv * (week.commission_rate + delta_commission_rate)
        commission_impact = new_commission - week.commission   # positive = more revenue

        projected = PnLWeek(
            week=week.week_dt,
            gmv=week.gmv,                     # volume unchanged (immediate reclassification)
            commission=new_commission,
            refund=week.refund,               # refund rate unchanged initially
            fulfillment_cost=week.fulfillment_cost,
            contribution_margin=week.contribution_margin + commission_impact,
        )
        weekly_pnl.append(projected)

    annual_cm_impact = sum(w.contribution_margin - b.contribution_margin
                          for w, b in zip(weekly_pnl, baseline.weekly_data)) * (52 / weeks)

    return SimulationResult(
        event_id=inputs.change_event.event_id,
        scenarios={'base': weekly_pnl},
        annualized_cm_impact=annual_cm_impact,
    )
```

---

## 7. Phase 4: Actual Impact Measurement (Post-launch)

After the change goes live, measure what **actually happened** — and separate the channel change signal from external noise (seasonality, macro trends, promotions running concurrently).

### Difference-in-Differences (DiD)

```
P&L impact = (Treatment_post - Treatment_pre) - (Control_post - Control_pre)

Treatment group: affected store(s) / channel
Control group:   similar stores that did NOT change (peer stores from Phase 3)
Pre period:      T-90d to T-0 (same as baseline)
Post period:     T+1 to T+13w
```

```python
def measure_actual_impact(event: ChangeEvent,
                          baseline: PnLBaseline,
                          post_launch_weeks: int = 13) -> ActualImpact:
    """
    Difference-in-Differences to isolate channel change signal.
    """
    post_launch_pnl = _query_actual_pnl(
        store_id=event.store_id,
        start_dt=event.effective_dt,
        weeks=post_launch_weeks,
    )

    # Control group: peer stores that didn't change in same period
    control_pre = _query_actual_pnl(
        store_ids=event.peer_stores,
        start_dt=baseline.start_dt,
        weeks=13,
    )
    control_post = _query_actual_pnl(
        store_ids=event.peer_stores,
        start_dt=event.effective_dt,
        weeks=post_launch_weeks,
    )

    # DiD calculation
    treatment_delta = post_launch_pnl.contribution_margin - baseline.contribution_margin
    control_delta = control_post.contribution_margin - control_pre.contribution_margin

    causal_impact = treatment_delta - control_delta   # channel change, stripped of market noise

    return ActualImpact(
        event_id=event.event_id,
        total_cm_delta=treatment_delta,
        causal_cm_impact=causal_impact,
        market_movement_cm=control_delta,           # how much was just the market
        attribution_breakdown={
            'volume_effect':       _volume_effect(baseline, post_launch_pnl),
            'commission_effect':   _commission_effect(baseline, post_launch_pnl),
            'refund_rate_effect':  _refund_effect(baseline, post_launch_pnl),
            'cost_effect':         _cost_effect(baseline, post_launch_pnl),
        }
    )
```

### Attribution decomposition

```
Total CM delta = Volume effect + Commission effect + Refund rate effect + Cost effect

Volume effect     = (actual_orders - baseline_orders) × baseline_cm_per_order
Commission effect = actual_orders × (new_commission_rate - baseline_commission_rate) × aov
Refund effect     = actual_orders × (new_refund_rate - baseline_refund_rate) × aov × (-1)
Cost effect       = actual_orders × (new_cost_per_order - baseline_cost_per_order) × (-1)

Check: sum of effects ≈ total CM delta (small residual = interaction effects)
```

---

## 8. Phase 5: Simulation vs Actual Reconciliation

Close the loop: how accurate was the pre-launch simulation?

```python
@dataclass
class SimAccuracyRecord:
    event_id:               str
    change_type:            str

    # What simulation predicted
    sim_base_cm_13w:        float
    sim_conservative_cm_13w: float
    sim_optimistic_cm_13w:  float

    # What actually happened
    actual_cm_13w:          float

    # Accuracy metrics
    mape:                   float    # mean absolute percentage error vs base case
    direction_correct:      bool     # did simulation get positive/negative right?
    actual_within_bounds:   bool     # actual between conservative and optimistic?

    # Calibration: update peer store model assumptions
    ramp_curve_actual:      List[float]  # actual weekly ramp achieved
    refund_rate_actual:     float
    error_drivers:          List[str]    # what caused the largest misses
```

```sql
-- Stored in BQ: US_FIN_ECOMM_DL_TABLES.PNL_SIMULATION_ACCURACY
-- Used to calibrate future simulations — improve ramp curves, refund rate priors

SELECT
    change_type,
    AVG(mape)                                    AS avg_mape,
    COUNTIF(direction_correct) / COUNT(*)        AS direction_accuracy,
    COUNTIF(actual_within_bounds) / COUNT(*)     AS within_bounds_rate,
    AVG(ramp_curve_actual[OFFSET(3)])            AS avg_week4_ramp   -- typical ramp at week 4
FROM US_FIN_ECOMM_DL_TABLES.PNL_SIMULATION_ACCURACY
GROUP BY change_type
```

---

## 9. Data Model

### Central registry: `PNL_IMPACT_EVENT`

```sql
-- Master table linking all phases for one change event
CREATE TABLE US_FIN_ECOMM_DL_TABLES.PNL_IMPACT_EVENT (
    event_id              STRING NOT NULL,
    change_type           STRING,           -- NEW_STORE | RECLASSIFIED | RETIRED | SPLIT
    store_id              STRING,
    old_channel           STRING,
    new_channel           STRING,
    effective_dt          DATE,
    detected_dt           DATE,

    -- Phase status
    baseline_frozen_ts    TIMESTAMP,
    simulation_run_ts     TIMESTAMP,
    impact_measured_ts    TIMESTAMP,
    reconciliation_ts     TIMESTAMP,

    -- Summary P&L impacts (for quick dashboard queries)
    sim_base_cm_impact    FLOAT64,          -- simulated CM delta (base case)
    actual_cm_impact      FLOAT64,          -- measured causal CM impact
    sim_accuracy_pct      FLOAT64,          -- abs % error between sim and actual

    -- Links
    baseline_table_ref    STRING,           -- BQ table/partition for baseline snapshot
    simulation_output_ref STRING,
    actual_impact_ref     STRING,

    analyst_approved      BOOL,             -- analyst signed off on simulation pre-launch
    analyst_id            STRING,
    approval_ts           TIMESTAMP
)
PRIMARY KEY (event_id) NOT ENFORCED
```

### Lineage

```
ChangeEvent (detected)
    └── PnLBaseline (frozen T-0 snapshot, 90d)
          ├── SimulationResult (forward projection, 3 scenarios)
          │     └── analyst approval → change goes live
          ├── ActualImpact (DiD measurement, post-launch)
          └── SimAccuracyRecord (sim vs actual, feeds calibration)
```

---

## 10. AI Layer — LLM-Assisted Analysis

Human analysts should not have to read raw numbers to understand what happened. The AI layer generates:

### Narrative generation (Haiku — high volume, templated)

```python
NARRATIVE_PROMPT = """
You are a Finance Data Analyst at Walmart eCommerce.

A channel hierarchy change occurred:
- Change type: {change_type}
- Store: {store_id} ({store_nm})
- Old channel: {old_channel} → New channel: {new_channel}
- Effective date: {effective_dt}

Simulation results (pre-launch prediction):
{simulation_summary}

Actual impact (post-launch measurement, {weeks_post} weeks):
{actual_impact_summary}

Attribution breakdown:
{attribution_breakdown}

Write a 3-paragraph executive summary:
1. What changed and when
2. What the financial impact has been (vs what we predicted)
3. Key drivers and any recommended actions

Use plain English. Include specific dollar amounts. Flag if actual deviated from simulation by >20%.
"""

def generate_narrative(event: ChangeEvent, sim: SimulationResult,
                       actual: ActualImpact) -> str:
    response = llm.messages.create(
        model='claude-haiku-4',   # Haiku — runs after every change, needs to be cheap
        messages=[{'role': 'user', 'content': NARRATIVE_PROMPT.format(...)}],
        max_tokens=600,
    )
    return response.content[0].text
```

### Anomaly detection + "Why?" agent (Opus — triggered only on large deviations)

```python
def analyze_deviation(event: ChangeEvent, sim: SimulationResult,
                      actual: ActualImpact) -> DeviationAnalysis:
    """
    Fires when |actual_cm - sim_base_cm| / sim_base_cm > 0.20 (>20% miss).
    Uses Opus to trace the deviation to root data changes.
    """
    deviation_pct = abs(actual.causal_cm_impact - sim.base_cm) / abs(sim.base_cm)

    if deviation_pct < 0.20:
        return None   # small miss — Haiku narrative is sufficient

    # Large miss — investigate with Opus
    deep_analysis = llm.messages.create(
        model='claude-opus-4',
        messages=[{
            'role': 'user',
            'content': DEEP_ANALYSIS_PROMPT.format(
                change_event=event,
                simulation=sim,
                actual=actual,
                refund_rate_delta=actual.attribution_breakdown['refund_rate_effect'],
                volume_delta=actual.attribution_breakdown['volume_effect'],
                comparable_events=_get_similar_past_events(event),
            )
        }],
        max_tokens=2000,
    )
    return _parse_deviation_analysis(deep_analysis)
```

### Root cause query agent

Analyst can ask: *"Why did the refund rate for store 9876 spike in week 3?"*

```python
TOOLS = [
    {
        "name": "query_bq",
        "description": "Run a BigQuery SQL query and return results",
        "input_schema": {"query": "string", "max_rows": "integer"},
    },
    {
        "name": "get_order_details",
        "description": "Get order-level detail for a store/date range",
        "input_schema": {"store_id": "string", "start_dt": "string", "end_dt": "string"},
    },
    {
        "name": "compare_to_peers",
        "description": "Compare a metric for store_id vs peer stores",
        "input_schema": {"store_id": "string", "metric": "string", "date_range": "string"},
    }
]

# Analyst asks a natural language question → agent calls tools → gives grounded answer
```

---

## 11. Output Layer — Analyst & Exec Surfaces

### Analyst self-serve (Scenario UI)

```
Input panel:
  ┌─────────────────────────────────────────────────────┐
  │ Change type: [NEW_STORE ▼]                          │
  │ Store ID: [_______]                                  │
  │ Effective date: [2026-09-01]                         │
  │                                                      │
  │ Volume ramp: [▓▓▓░░░░░░░░░░] (drag to adjust)       │
  │ Commission rate: [0.12] → [0.15]                     │
  │ Peer stores (proxy): [9101, 9202, 9303]              │
  └─────────────────────────────────────────────────────┘

Output: 3-scenario P&L waterfall chart (13-week projection)
        Break-even week indicator
        Annualized CM impact: $X.XM (conservative) to $X.XM (optimistic)
        AI narrative: 3-paragraph plain-English summary
```

### Exec dashboard (BigQuery → Looker Studio)

```
Page 1: Active Changes
  - Table of all open change events (in simulation or post-launch phases)
  - Traffic light: simulation prediction vs actual trajectory

Page 2: P&L Impact Summary
  - Total simulated CM impact across all active changes: $XXM
  - Total measured causal CM impact YTD: $XXM
  - Simulation accuracy score: XX% (MAPE)

Page 3: Drill-down per event
  - Waterfall: attribution breakdown
  - Simulation vs actual overlay chart
  - AI narrative + anomaly flags
```

### Downstream API

```python
# FastAPI / Cloud Run endpoint — consumed by forecasting models

GET /api/v1/pnl-impact/{event_id}
  → SimulationResult + ActualImpact + Accuracy

GET /api/v1/pnl-impact/store/{store_id}/history
  → List of all change events + outcomes for a store

POST /api/v1/pnl-impact/simulate
  Body: SimulationInputs
  → SimulationResult (synchronous, <5s)

GET /api/v1/pnl-impact/accuracy?change_type=NEW_STORE
  → Aggregated simulation accuracy metrics (feeds forecast model calibration)
```

---

## 12. Implementation Roadmap

| Phase | What | Deliverable | Duration |
|-------|------|-------------|----------|
| **P0** | Channel Hierarchy Master table + CDC detection | `CHANNEL_HIERARCHY_MASTER` BQ table, daily diff job | 1 week |
| **P1** | Baseline freezer | `PNL_IMPACT_BASELINE` snapshot on change detect | 1 week |
| **P2** | Reclassification simulation | Deterministic P&L delta for commission/cost changes | 2 weeks |
| **P3** | New store simulation | Ramp curve model, peer-store proxy, 3-scenario output | 3 weeks |
| **P4** | Actual impact measurement (DiD) | Post-launch DiD calculator, attribution breakdown | 3 weeks |
| **P5** | Haiku narrative generation | Auto-generated plain-English summaries per event | 1 week |
| **P6** | Analyst self-serve UI | Scenario input panel + waterfall chart (Streamlit or Looker) | 3 weeks |
| **P7** | Exec dashboard | Looker Studio / BQ dashboard | 2 weeks |
| **P8** | Simulation accuracy registry + calibration | Accuracy tracking, ramp curve learning | 2 weeks |
| **P9** | Opus deviation analysis agent | Deep-dive for >20% simulation misses | 2 weeks |

**MVP (P0–P5): ~11 weeks** — simulation + measurement + AI narrative, no UI
**Full system (P0–P9): ~20 weeks**

---

## Open Questions (to confirm)

1. **Is `CHANNEL_HIERARCHY_MASTER` an existing BQ table or does it need to be created?** If it already exists, CDC detection is faster to build.
2. **What is the source of fulfillment cost at store level?** SAP GL via sub-ledger API, or something else?
3. **Is the P&L simulation input to Anaplan?** If yes, the output format needs to match Anaplan import schema.
4. **Who approves simulations before launch?** Finance VP? This determines the approval workflow UI.
5. **Is the 90-day baseline window right?** For stores with high seasonality (holiday Q4), a 52-week baseline with seasonal decomposition may be more accurate.

---

*Designed for Walmart Finance Engineering — GCP BigQuery, Airflow (FDL), OMS2, Plutus/Kafka*
*AI: Claude Haiku 4 (narrative, high-volume), Claude Opus 4 (deviation analysis, triggered)*
