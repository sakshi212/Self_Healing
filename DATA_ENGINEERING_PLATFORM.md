# eCommerce Data Engineering Platform Architecture
### Near-Real-Time + Batch · Medallion Architecture · AI-Assisted Self-Healing
### Walmart Finance Engineering

---

## Table of Contents

1. [Platform Overview](#1-platform-overview)
2. [Design Challenges](#2-design-challenges)
3. [Ingestion Layer](#3-ingestion-layer)
4. [Streaming Processing — Spark Structured Streaming](#4-streaming-processing--spark-structured-streaming)
5. [Medallion Architecture — Bronze / Silver / Gold](#5-medallion-architecture--bronze--silver--gold)
6. [Orchestration & Reliability](#6-orchestration--reliability)
7. [Observability](#7-observability)
8. [Cost Optimization](#8-cost-optimization)
9. [CI/CD & Deployment](#9-cicd--deployment)
10. [AI-Assisted Self-Healing](#10-ai-assisted-self-healing)
11. [Engineering Excellence Metrics](#11-engineering-excellence-metrics)
12. [Architecture Diagram — Full Platform](#12-architecture-diagram--full-platform)

---

## 1. Platform Overview

This platform processes **eCommerce sales and transaction data** for Walmart's eCommerce organization, combining near-real-time streaming with batch sources into a unified, reliable data foundation that serves analytics, business metrics, and downstream financial reporting.

**Core design principles:**

| Principle | How it's implemented |
|-----------|---------------------|
| **Eventual completeness** | Streaming for NRT availability; reconciliation against batch feeds guarantees nothing is permanently missed |
| **Immutable source of truth** | Bronze layer preserves raw events in GCS — every failure is recoverable by replaying from source |
| **Fail early on bad data** | Data contracts validated at ingestion — schema violations and quality failures caught before propagating downstream |
| **Idempotency everywhere** | Every pipeline step can be safely re-run — no duplicate data, no side effects from retries |
| **AI on top of guardrails** | Self-healing sits on top of deterministic controls, not instead of them |

---

## 2. Design Challenges

The platform was not simply a streaming problem. Five distinct challenges required explicit architectural decisions:

**Challenge 1 — Late-arriving and duplicate events**
Kafka consumer groups can reprocess events on rebalance. Streaming sources deliver events out of order. Batch feeds for the same data arrive hours later. Without explicit handling, these produce duplicate records and incorrect aggregates.

**Challenge 2 — Data contracts at scale**
With multiple upstream producers (OMS, Plutus/Kafka, SAP, external marketplace APIs), each with independent schema evolution, bad data propagation is a constant risk. A schema change in one upstream system silently breaks downstream Gold metrics if not caught at ingestion.

**Challenge 3 — NRT vs batch reconciliation**
Streaming provides low latency but incomplete windows. Batch provides completeness but higher latency. Business metrics need both — NRT for operational dashboards, batch-reconciled for financial reporting. The platform must serve both without maintaining two separate data models.

**Challenge 4 — Cost at scale**
eCommerce transaction volume is highly seasonal — Black Friday is 10–20× normal volume. Overprovisioning for peak wastes 95% of the year. Underprovisioning creates processing lag during peak. Static cluster sizing doesn't work.

**Challenge 5 — Market-level deployment isolation**
A bug in Canadian pipeline logic must not create deployment risk for US pipelines. A failed deployment in one market must not block other markets from shipping. Standard monolithic CI/CD creates exactly this coupling.

---

## 3. Ingestion Layer

```
┌───────────────────────────────────────────────────────────────────────────┐
│  UPSTREAM SOURCES                                                          │
│                                                                            │
│  OMS (Order Mgmt)   Plutus/Kafka   SAP GL    Marketplace APIs   Batch FTP │
└────────┬────────────────┬──────────────┬───────────┬──────────────────────┘
         │                │              │           │
         ▼                ▼              │           ▼
┌─────────────────────────────┐         │   ┌──────────────────┐
│  KAFKA (Streaming Sources)  │         │   │  BATCH INGESTION  │
│                             │         │   │  GCS landing zone │
│  Topics per domain:         │         │   │  Scheduled SFTP / │
│  · orders.raw               │         │   │  API pulls        │
│  · payments.raw             │         │   └────────┬─────────┘
│  · returns.raw              │         │            │
│  · inventory.raw            │         └────────────┤
└────────────┬────────────────┘                      │
             │                                       │
             ▼                                       ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  DATA CONTRACT VALIDATION LAYER  (fires before Bronze write)             │
│                                                                          │
│  For every event / batch record:                                         │
│  1. Schema compatibility check  — does this message match the registered │
│     schema version? Any added/removed/type-changed fields?               │
│  2. Mandatory attribute check   — are required fields present and        │
│     non-null? (order_id, event_ts, store_id, amount)                     │
│  3. Basic quality rules         — amount > 0, event_ts not in future,   │
│     store_id in known hierarchy, order_id format valid                   │
│                                                                          │
│  PASS → write to Bronze                                                  │
│  FAIL → route to Dead Letter Queue (DLQ) + alert + do NOT propagate     │
└─────────────────────────────────────────────────────────────────────────┘
```

### Kafka Consumer Design — Idempotency & Duplicates

Kafka consumers are designed to be **idempotent** — re-processing the same message produces the same result, not a duplicate record.

| Problem | Design decision |
|---------|----------------|
| **Duplicate delivery** (Kafka at-least-once) | Deduplication key on `(event_id, event_ts, store_id)` — applied at Bronze write and again at Silver dedup step |
| **Consumer rebalance reprocessing** | Offsets committed only after successful Bronze write — no partial commits |
| **Late-arriving events** | Accepted into Bronze regardless of lateness — watermark and reconciliation handle them downstream |
| **Poison messages** (unparseable) | Routed to DLQ with full original payload preserved — replayable after producer fix |

### Dead Letter Queue (DLQ)

Every ingestion path has a DLQ. Failed records are:
- Preserved in full with original payload, failure reason, and timestamp
- Never silently dropped
- Monitored — DLQ depth is a primary alert metric
- Replayable after the upstream issue is resolved

---

## 4. Streaming Processing — Spark Structured Streaming

```
Kafka topic
    │
    ▼
Spark Structured Streaming job
    │
    ├─ Event time processing (not processing time)
    │   → uses event_ts from the message, not Spark's wall clock
    │   → prevents reprocessing artifacts when jobs restart
    │
    ├─ Watermarking for late data
    │   → watermark = max observed event_ts − tolerance window
    │   → events within watermark: included in aggregates
    │   → events beyond watermark: accepted into Bronze, flagged as late,
    │       handled in batch reconciliation (not silently dropped)
    │
    ├─ Stateful aggregations (per window, per store, per channel)
    │   → intermediate state checkpointed to GCS
    │   → checkpoints enable restart-without-reprocessing
    │
    └─ Output: write to Bronze (GCS) + trigger Silver micro-batch
```

### Late-Arriving Data — Two-Layer Strategy

No single watermark tolerance handles all business requirements. A 10-minute watermark works for operational dashboards; financial reporting requires completeness across a full day.

```
Layer 1: Streaming (NRT, low-latency, incomplete window)
  → Watermark: 30 minutes
  → Serves: operational dashboards, real-time alerts
  → SLA: data available within minutes of event time
  → Completeness: ~95% of events within the window

Layer 2: Batch reconciliation (high-latency, complete)
  → Runs: hourly / daily depending on the feed
  → Reads: Bronze (immutable) + batch source feeds
  → Reconciles: any events the streaming window missed
  → Serves: financial reporting, regulatory data, P&L
  → Completeness: 100% (given source system completeness)

The Silver layer merges both — streaming results updated by reconciliation.
Business metrics in Gold query Silver, which always reflects the latest reconciled state.
```

---

## 5. Medallion Architecture — Bronze / Silver / Gold

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  BRONZE — Immutable Raw Layer  (GCS)                                         │
│                                                                              │
│  · Stores events exactly as received — no transformation, no cleansing      │
│  · Partitioned by event_date / source / market                              │
│  · Schema-on-read — preserves original structure even if schema evolves     │
│  · Retention: configurable per domain (orders: 7 years for compliance)      │
│  · Purpose: replayable source of truth — any downstream failure can be      │
│    recovered by re-processing from Bronze                                   │
│                                                                              │
│  Writes: Kafka consumer (streaming) + batch ingestion jobs                  │
│  Format: Parquet (compressed) + Hudi CoW for mutable event corrections      │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  SILVER — Cleansed & Trusted Layer  (GCS + BigQuery)                         │
│                                                                              │
│  Applied transformations (in order):                                        │
│  1. Deduplication      — remove duplicate events by (event_id, event_ts)   │
│  2. Late-arrival merge — streaming results + batch reconciliation merged    │
│  3. Data cleansing     — nulls handled, types normalized, formats standard  │
│  4. Business transforms — apply business rules: refund netting, commission  │
│     rate application, channel hierarchy mapping                             │
│  5. Schema standardization — canonical column names across all markets      │
│                                                                              │
│  Output: trusted datasets consumable by Gold or directly by analysts        │
│  SLA: streaming path: <5 min lag; reconciled path: complete by T+1h        │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  GOLD — Curated Business Models  (BigQuery)                                  │
│                                                                              │
│  · Pre-aggregated KPIs and metrics with consistent business definitions     │
│  · Dimensions and facts organized for analytics consumption                 │
│  · Single definition per metric — no divergent calculations across teams    │
│                                                                              │
│  Examples:                                                                  │
│  · GMV by channel / store / day (with refund netting)                      │
│  · Contribution margin by fulfillment type                                  │
│  · Refund rate by seller / category                                         │
│  · Order-to-ship latency distribution                                       │
│                                                                              │
│  Consumers: BI dashboards, Finance reporting, Data science, APIs            │
│  Refresh: streaming-updated NRT tables + daily batch-reconciled tables      │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Key design decision — why Bronze is immutable

Every time a downstream failure occurs, the question is: can we recover? With immutable Bronze, the answer is always yes — re-process from Bronze. Without it, a Silver or Gold corruption may have no clean source to rebuild from. Bronze immutability is the reliability foundation everything else rests on.

---

## 6. Orchestration & Reliability

**Airflow** orchestrates all batch and scheduled streaming jobs. Every pipeline is designed with four reliability properties:

### Retry
Every task has a retry policy with exponential backoff. Transient failures (network timeout, cluster preemption) resolve themselves on retry without human intervention. Retry counts, delays, and max attempts are configured per task type — not a single global default.

### Dependency management
DAG dependencies are explicit — a Gold aggregation job cannot run until the Silver reconciliation it depends on has completed successfully. Airflow's sensor operators wait for upstream completion signals (touch files on GCS, BQ table availability) rather than using fixed time delays.

### Idempotency
Every pipeline step is designed to be safely re-run:
- BQ loads use `WRITE_TRUNCATE` or `MERGE` — never `WRITE_APPEND` without deduplication
- Hudi CoW tables upsert by primary key — re-running doesn't produce duplicate rows
- Touch files are overwritten, not appended — no state accumulation from retries
- Any step that isn't idempotent is flagged as a circuit-breaker risk and treated specially

### Recovery
When a pipeline fails mid-run, recovery is deterministic:
1. Failed tasks are automatically retried (transient failures)
2. If retries exhaust: AI-assisted RCA workflow activates (see §10)
3. Downstream DAGs that read from this pipeline's outputs are paused immediately to prevent consuming stale or incomplete data
4. After fix: replay from Bronze if data integrity is uncertain; otherwise clear and re-trigger from the failed point

---

## 7. Observability

All pipeline and infrastructure metrics are published to **Grafana** dashboards. Observability is organized in three layers:

### Layer 1 — Pipeline health
| Metric | Alert threshold | Why it matters |
|--------|----------------|---------------|
| Processing lag (streaming) | > 5 minutes | NRT SLA breach |
| DLQ depth | > 0 for > 10 min | Bad data being dropped silently |
| Task failure rate | > 2% per hour | Systematic failure pattern |
| DAG completion time | > 120% of p95 baseline | Pipeline slowdown detection |
| Data freshness (Gold tables) | > configured SLA per table | Downstream consumers seeing stale data |

### Layer 2 — Infrastructure efficiency
| Metric | Why it matters |
|--------|---------------|
| CPU utilization per cluster | Identifies idle compute to right-size |
| Memory utilization per executor | Detects OOM risk before it causes failure |
| GCS read/write throughput | Detects I/O bottlenecks in data-intensive jobs |
| Cluster autoscaling events | Validates autoscaling is responding correctly to load |
| Shuffle spill to disk | Leading indicator of under-provisioned memory |

### Layer 3 — Business data quality
| Metric | Why it matters |
|--------|---------------|
| Record count delta vs prior day | Detects upstream data drops or unexpected volume spikes |
| Null rate per key column | Detects upstream schema drift |
| Reconciliation gap (streaming vs batch) | Measures completeness of the streaming window |
| Metric value anomalies (GMV, order count) | Catches data corruption before it reaches finance reports |

**Grafana alert routing:**
- P1 (data corruption risk, DLQ spike, Gold SLA breach) → PagerDuty + Slack
- P2 (processing lag, cluster utilization) → Slack alert channel
- P3 (trend anomalies, efficiency degradation) → daily digest email

---

## 8. Cost Optimization

### Autoscaling
Dataproc clusters scale worker node count based on YARN pending container metrics. Scale-up triggers when jobs are queuing; scale-down triggers when utilization falls below threshold for a sustained period. This handles the 10–20× peak volume during Black Friday without statically provisioning for that capacity year-round.

### Right-sizing discipline
Grafana utilization metrics feed a weekly right-sizing review:
- Clusters consistently running at < 40% CPU → downsize
- Jobs consistently OOM-ing → upsize executor memory before failure becomes a reliability issue
- Jobs with high shuffle spill → tune `spark.sql.shuffle.partitions` rather than adding workers

### Compute tiering
| Workload type | Compute choice | Reason |
|--------------|---------------|--------|
| Streaming (continuous) | Persistent small cluster | Startup latency unacceptable for NRT |
| Heavy batch transforms | Ephemeral autoscaling cluster | Created per run, deleted on completion |
| Light batch / BQ SQL | BigQuery serverless | No cluster management, pay per query |
| Great Expectations DQ checks | Ephemeral small cluster | Runs infrequently, short duration |

### Storage cost controls
- Bronze: compressed Parquet with lifecycle rules (hot → nearline → coldline by age)
- Silver: intermediate results pruned after Gold aggregation is confirmed
- Gold: BigQuery table partitioning + clustering to minimize bytes scanned per query

---

## 9. CI/CD & Deployment

### Market-level isolation

The most important deployment architecture decision: changes are scoped and deployed per market (US, Canada, Mexico, etc.) independently.

```
Feature branch
      │
      ▼
PR created
      │
      ▼
Automated test suite runs (scoped to changed markets only)
  ├─ Unit tests         — logic correctness
  ├─ Integration tests  — pipeline wiring, BQ schema, GCS paths
  ├─ Functional tests   — end-to-end pipeline with synthetic data
  └─ Data quality tests — Great Expectations suite for output datasets
      │
      ▼  All pass
Deploy to staging (affected markets only)
      │
      ▼
Production smoke tests (affected markets only)
  → Sample records flow end-to-end
  → Gold metric values within expected range vs prior day
  → DLQ empty after smoke run
      │
      ├─ Pass → deploy confirmed, monitoring continues
      │
      └─ Fail → automated rollback to last stable pipeline version
                (no manual intervention needed for rollback)
```

**Why market isolation matters:** A bug in Canadian order processing logic would, in a monolithic CI/CD system, block US deployments or worse, deploy broken logic to US pipelines. Market isolation contains blast radius — a Canadian deployment failure has zero effect on US pipeline availability or deployment velocity.

### Automated rollback

Rollback is not a manual runbook step. When production smoke tests fail:
1. The new pipeline version is automatically deactivated
2. The last confirmed-stable version is reactivated
3. Airflow is updated to point to the stable version
4. Alert sent to the deploying team with smoke test failure details
5. The failed version is quarantined for investigation — not automatically retried

### Great Expectations data quality gates

Great Expectations runs at two points in the deployment pipeline:

| Gate | What it checks | Blocks if |
|------|---------------|-----------|
| Pre-deploy (on PR) | Schema expectations for all output datasets — column types, nullable constraints, expected value ranges | Any expectation fails on synthetic test data |
| Post-deploy (smoke test) | Row count reasonableness, null rates, metric value ranges against prior day actuals | Significant deviation from baseline detected |

---

## 10. AI-Assisted Self-Healing

Self-healing sits **on top of** deterministic controls, observability, and operational guardrails — not instead of them. The layering matters: no AI agent touches production unless the deterministic layers have already failed to resolve the issue.

### Decision framework — what gets automated vs. escalated

```
Production failure detected
         │
         ▼
Is this deterministic and safe to automate?
  (transient network failure, resource contention,
   known retry-safe failure pattern)
         │
    YES  │  NO
         │
    ▼    │    ▼
Auto-    │  AI-assisted RCA workflow
retry    │
```

**Deterministic automation handles:** transient network failures, cluster preemption and recreation, resource quota recoveries, touch-file timeouts where upstream completed late, known idempotent retry scenarios.

**AI-assisted RCA handles:** novel failures, complex multi-system failures, failures where the cause isn't visible in the immediate task log.

### AI-Assisted RCA Context Collection

When a complex failure requires RCA, the agent collects targeted context — not a bulk log dump:

| Context source | What's collected | Why bounded |
|---------------|-----------------|-------------|
| Execution logs | Pre-processed extract: ~50 lines around the failure signal (exception, stack trace, exit code) — not the full 100K-line log | Signal without noise |
| Lineage | DAG topology for this job: immediate upstream/downstream dependencies, datasets read/written | Not the full pipeline graph |
| Infrastructure metrics | Cluster utilization, memory, CPU at time of failure | Scoped to this run |
| Deployment history | Code and config changes in the 48 hours before failure | Bounded to relevant change window |
| Historical incidents | Top 3–5 most similar past failures with their root causes and resolutions | Retrieved, not embedded in prompt |

### Confidence-Based Remediation Routing

After the agent identifies the likely root cause and downstream blast radius, remediation is routed by confidence:

```
HIGH confidence, LOW risk
(transient failure, known pattern, successful in >90% of historical cases)
   → Automated: retry with parameter adjustment, resource scale-up
   → No human required in the happy path

MEDIUM confidence
(similar to past incidents but with uncertainty in the fix approach)
   → Pull request generated with: proposed fix, RCA context, supporting evidence
   → Engineer reviews and approves before production deployment
   → RCA already assembled — engineer spends minutes reviewing, not hours triaging

LOW confidence / novel failure
   → Escalation to engineer with full RCA context assembled
   → Engineer has: failure summary, blast radius, retrieved similar incidents,
     what the agent tried and why it was uncertain
   → Engineer resolves with context already in hand — not starting from zero
```

### Blast Radius Identification

Before any remediation, the agent identifies which downstream pipelines and datasets are affected:
- BFS traversal of the DAG lineage graph (Neo4j)
- Identifies all downstream consumers of datasets written by the failed pipeline
- Those downstream pipelines are paused immediately — preventing consumption of stale or incomplete data
- Resume order is topological (hop-by-hop from the root fix) after resolution

### What AI does NOT do

- Does not modify production systems directly — all changes go through PR + approval
- Does not treat its own hypothesis as evidence — every conclusion must cite specific log signals, retrieved incidents, or infrastructure state
- Does not auto-merge PRs — engineer is the mandatory approval gate for any production deployment
- Does not attempt to fix auth/credential failures (R3) or certificate issues (R4) — those require secret rotation by a human and are immediately escalated

---

## 11. Engineering Excellence Metrics

The platform's effectiveness is measured quantitatively across four dimensions:

### Reliability
| Metric | Definition | Target |
|--------|-----------|--------|
| **MTTD** (Mean Time to Detect) | Time from failure occurrence to alert firing | < 5 minutes |
| **MTTR** (Mean Time to Resolve) | Time from alert to pipeline back in healthy state | < 30 minutes (auto-resolved) / < 4 hours (engineer-resolved) |
| **Pipeline reliability** | % of scheduled pipeline runs completing successfully on first attempt | > 99.5% |
| **Data freshness SLA compliance** | % of Gold tables delivered within their stated SLA | > 99% |
| **Cascading failure rate** | % of failures that propagate to downstream pipelines | < 1% (target: 0 with BFS pause) |

### Automation effectiveness
| Metric | Definition | Target |
|--------|-----------|--------|
| **Auto-resolution rate** | % of failures resolved without engineer intervention | > 60% |
| **AI remediation rate** | % of AI-generated PRs merged without modification | > 70% |
| **False positive rate** | % of auto-resolutions that caused a new failure | < 2% |
| **Alert noise reduction** | Reduction in alert volume vs. baseline | > 80% |

### Data quality
| Metric | Definition |
|--------|-----------|
| **DLQ rate** | % of ingested records routed to DLQ (target: < 0.1%) |
| **Reconciliation gap** | Events in batch not captured by streaming (target: < 0.5%) |
| **Schema contract violations** | Count of upstream schema breaks caught at ingestion per week |
| **Data quality test pass rate** | Great Expectations suite pass rate in CI (target: 100% required) |

### Infrastructure efficiency
| Metric | Definition | Target |
|--------|-----------|--------|
| **Cluster utilization** | Average CPU/memory utilization across all compute | 65–80% (avoids both waste and saturation) |
| **Autoscaling efficiency** | % of peak workloads handled without manual intervention | > 95% |
| **Cost per GB processed** | Cloud spend normalized to data volume | Trending down QoQ |

---

## 12. Architecture Diagram — Full Platform

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  UPSTREAM SOURCES                                                                │
│  OMS · Plutus/Kafka · SAP GL · Marketplace APIs · Batch FTP                    │
└──────────────┬──────────────────────────────────────┬───────────────────────────┘
               │ streaming                             │ batch
               ▼                                      ▼
┌──────────────────────────┐             ┌────────────────────────────┐
│  KAFKA                   │             │  GCS LANDING ZONE          │
│  Topics: orders, payments│             │  Scheduled pulls / SFTP    │
│  returns, inventory      │             │  Validated on arrival      │
└──────────────┬───────────┘             └─────────────┬──────────────┘
               │                                       │
               └──────────────────┬────────────────────┘
                                  │
                                  ▼
                   ┌──────────────────────────────┐
                   │  DATA CONTRACT VALIDATION    │
                   │  Schema · Mandatory attrs    │
                   │  Quality rules · DLQ routing │
                   └──────────────┬───────────────┘
                                  │
               ┌──────────────────┴──────────────────┐
               │ streaming path                       │ batch path
               ▼                                      ▼
┌──────────────────────────┐             ┌────────────────────────────┐
│  SPARK STRUCTURED        │             │  AIRFLOW-ORCHESTRATED      │
│  STREAMING               │             │  BATCH JOBS                │
│  Event time + watermark  │             │  GCS → Bronze writer       │
│  Stateful aggregations   │             │  Scheduled per feed        │
│  Checkpointed to GCS     │             └─────────────┬──────────────┘
└──────────────┬───────────┘                           │
               └──────────────────┬────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  BRONZE  (GCS — immutable, partitioned, compressed Parquet / Hudi CoW)         │
│  Replayable source of truth for all downstream processing                       │
└──────────────────────────────────┬──────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  SILVER  (GCS + BigQuery)                                                       │
│  Dedup · Late-arrival merge · Cleansing · Business transforms · Reconciliation │
└──────────────────────────────────┬──────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  GOLD  (BigQuery)                                                               │
│  Curated KPIs · Pre-aggregated metrics · Consistent business definitions       │
└──────────┬──────────────────────────────────────────────────────────────────────┘
           │
           ├──→ BI Dashboards (Looker / Tableau)
           ├──→ Finance Reporting (P&L, reconciliation)
           ├──→ Data Science / ML feature store
           └──→ APIs (downstream product teams)

┌─────────────────────────────────────────────────────────────────────────────────┐
│  PLATFORM LAYER (horizontal — applies across all above)                         │
│                                                                                 │
│  Airflow orchestration     │ Retry · Dependency · Idempotency · Recovery       │
│  Grafana observability     │ Pipeline health · Infra efficiency · Data quality  │
│  CI/CD (market-isolated)   │ Unit → Integration → Functional → GX DQ → Smoke  │
│  Autoscaling               │ Dataproc YARN-based · Ephemeral batch clusters     │
│  AI Self-Healing           │ Deterministic first · AI on top · Human gate      │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

*Platform: GCP (Dataproc, BigQuery, GCS, Pub/Sub) · Apache Kafka · Spark Structured Streaming · Apache Hudi · Apache Airflow · Grafana · Great Expectations · Neo4j · Gemini 2.5*
