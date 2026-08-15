# Architecture Review Board — High Level Design
## AI-Assisted Self-Healing eCommerce Data Pipeline Platform

---

| Field | Value |
|-------|-------|
| **Initiative Name** | AI-Assisted Self-Healing eCommerce Data Pipeline Platform |
| **Document Status** | IN-PROGRESS |
| **Epic Jira Labels** | `Finance-DE-SelfHeal-v1` |
| **Related Resources** | [AI_Based_Self_Healing.md](./AI_Based_Self_Healing.md) · [DATA_ENGINEERING_PLATFORM.md](./DATA_ENGINEERING_PLATFORM.md) |
| **Short Description** | Finance-eComm and Sam's Club run 450+ Airflow/Dataproc pipeline DAGs processing sales, orders, and financial transaction data. Engineers spend 800+ hours/year on manual triage of pipeline failures. This initiative introduces an AI-assisted, layered self-healing platform that classifies failures, retrieves historical resolutions, performs agentic RCA, and autonomously remediates or escalates based on confidence — with human approval required before any production deployment. |

---

## Contact Reference

| Role | POC |
|------|-----|
| **Program Manager** | TBD |
| **Product Manager** | TBD |
| **Tech Lead** | TBD |
| **Architect** | Sakshi (s0s03n9) |
| **Security / SSP** | TBD |

---

## 1. Problem Statement and ROI

### Problem Statement

Finance-eComm and Sam's Club Finance run **450+ PySpark/Spark pipelines** across Airflow on GCP. These pipelines process sales orders, payments, refunds, inventory, and financial allocation data that feeds downstream reporting, P&L analysis, SAP reconciliation, and business dashboards.

**Current operational reality:**

| Step | Manual Activity | Time per incident |
|------|----------------|-------------------|
| Detection | Outlook alert → Airflow UI | 5–15 min |
| Impact Assessment | Trace DAG lineage manually (tribal knowledge) | 15–30 min |
| Containment | Pause dependent DAGs one-by-one in UI | 10–20 min |
| Root Cause Analysis | Read raw Dataproc/Spark logs (100K+ lines) | 30–90 min |
| Fix Development | Write fix, test locally, raise PR | 30–60 min |
| Resolution | Deploy, clear failed run, re-trigger DAG + dependents | 30–60 min |

- **Total per incident:** 2–5 hours
- **Annual engineering overhead:** 800+ hours/year (~9+ failures/day)
- **Highest risk:** Cascading data corruption — if a failed upstream DAG isn't contained within minutes, its downstream consumers produce silently corrupted financial reports

**Specific failure patterns driving the highest cost:**

1. Delete Cluster tasks failing (cluster already deleted = 404; auto-restart replays same broken state)
2. `max_active_runs: 1` zombie runs blocking all scheduled runs
3. OOM errors on Dataproc executors under seasonal volume spikes
4. Package/dependency mismatches after library upgrades
5. Auth/token expiry (GEC GitHub PAT, SA keys) — highest frequency, non-auto-fixable

### ROI and Key Results

| Metric | Before | After (target) |
|--------|--------|---------------|
| Failure containment time | 15–30 min | 2–5 seconds (BFS downstream pause) |
| Root cause analysis time | 30–90 min | < 5 min (AI-assisted) |
| Resolution time (auto-resolved) | 2–5 hours | 5–15 min |
| Annual engineering overhead | 800+ hours/yr | ~80 hours/yr (PR review only) |
| Cascading data corruption incidents | 10–20/year | 0 (exhaustive BFS lineage pause) |
| Alert noise | High (every downstream failure = alert) | 80% reduction |
| Auto-resolution rate | 0% | > 60% target |

**Quantified annual savings:**
- **720+ engineering hours** unlocked → redeployed to feature development
- **10–20 data corruption incidents** prevented → no emergency report corrections or finance escalations
- **Finance data freshness SLA** maintained without on-call manual intervention

---

## 2. Business Use-Cases (Testable)

| # | Actor | Condition | Expected Outcome |
|---|-------|-----------|-----------------|
| UC-1 | Finance pipeline | Dataproc Delete Cluster task fails with 404 (cluster already gone) | System detects 404, treats as success (idempotent), proceeds to next task without human intervention. No engineer alert required. |
| UC-2 | Finance pipeline | DAG stuck in RUNNING state > 6 hours (zombie run) | Watchdog detects zombie, marks run FAILED via Airflow REST API, `max_active_runs: 1` slot freed, next scheduled run proceeds automatically. |
| UC-3 | Finance pipeline | OOM error on Dataproc executor | System detects R1 bucket via regex, scales executor memory via Airflow Variable, retries task with new config. No human required. |
| UC-4 | Finance pipeline | Novel PySpark failure not in history | Log pre-processor extracts 2–5K char bounded extract; embedding classifier routes to Gemini; Gemini performs agentic RCA with tool calls; returns structured response with ranked remediations. |
| UC-5 | Finance pipeline | One upstream DAG fails, 12 downstream DAGs depend on it | Within 2–5 seconds, all 12 downstream DAGs are paused via BFS traversal. No downstream DAG consumes stale or incomplete data. After root fix, DAGs resume in topological hop order. |
| UC-6 | Engineering team | AI proposes a code fix with confidence ≥ 0.90, sandbox tests pass, Snyk clean | PR auto-created in GitHub with error context, proposed diff, test results, confidence score. Engineer reviews and approves (mandatory) before production merge. |
| UC-7 | Engineering team | AI proposes a fix but confidence = 0.55 (ambiguous retrieval) | No auto-execution. Engineer receives escalation alert with RCA context already assembled — failure summary, blast radius, top 3 similar historical incidents, what Gemini concluded and why it was uncertain. |
| UC-8 | Finance pipeline | Auth/credential failure (token expired, SA key revoked) | System routes to R3 bucket (non-auto-fixable), sends ops escalation email immediately with runbook link. No auto-fix attempted. |
| UC-9 | Engineering team | Same failure type occurs 4th time in 24 hours | Circuit breaker opens — stops all auto-healing for that DAG/bucket, sends escalation. Prevents infinite retry loops on systematic failures. |
| UC-10 | Finance pipeline | AI-proposed fix merged, pipeline succeeds | Outcome written back to Neo4j knowledge graph. Future similar incidents use this resolution without LLM call. Confidence calibration improves over time. |

---

## 3. Functional Tech Requirements (Testable)

| # | Requirement | Test Condition | Expected Outcome |
|---|-------------|---------------|-----------------|
| FR-1 | `on_failure_callback` fires on every DAG task failure | Trigger any task failure in staging | HTTP POST sent to Heal Server within 5 seconds |
| FR-2 | BFS lineage traversal pauses all downstream dependents | Fail an upstream DAG with 5 known dependents | All 5 downstream DAGs paused within 5 seconds via Airflow REST API |
| FR-3 | Log pre-processor reduces raw logs to 2–5K char extract | Submit 100K-line Dataproc log | Output extract ≤ 5K chars containing exception, stack trace, file candidates, exit code |
| FR-4 | Regex classifier (v0) correctly routes known error patterns | Submit logs for each of 9 error buckets (R1–R9) | Each routed to correct bucket with confidence ≥ 0.90 for known patterns |
| FR-5 | Embedding classifier (v1) handles semantic variants of known errors | Submit 3 different phrasings of OOM error | All 3 classified as R1 with confidence ≥ 0.85 |
| FR-6 | P_vector penalized when retrieved incidents disagree on bucket | Retrieve 3 incidents: 2 R2 + 1 R1 | confidence < 0.70; routes to Gemini, no auto-execution |
| FR-7 | Gemini RCA returns Pydantic-structured response | Trigger novel failure; route to Gemini | Response validates against schema: problem_summary, supporting_evidence (cited), root_cause_hypothesis, confidence_score, remediation_options, evidence_sufficient |
| FR-8 | evidence_sufficient=FALSE triggers ops escalate | Provide Gemini insufficient log context | Returns evidence_sufficient=FALSE; no remediation proposed; ops escalation fired |
| FR-9 | Auto-retry changes a parameter before retrying | Trigger OOM (R1) failure | System scales executor memory via Airflow Variable BEFORE triggering retry — not a plain replay |
| FR-10 | Sandbox testing (unit tests) runs before PR creation | Trigger code fix path | Unit tests run via pyspark local mode; PR created only if tests pass |
| FR-11 | Snyk scan and secret detection gate PR creation | Inject CVE into proposed fix | PR not created; engineer notified with scan result |
| FR-12 | PR auto-created with confidence ≥ 0.90, all gates pass | End-to-end code fix with high confidence | GitHub PR created with error summary, diff, test results, confidence score; engineer assigned as reviewer |
| FR-13 | Circuit breaker opens after 3 auto-attempts/24h | Trigger same failure 4 times in 24 hours | 4th attempt blocked; escalation sent; DAG circuit marked OPEN |
| FR-14 | Watchdog detects zombie runs | Create DAG run stuck in RUNNING for 7 hours | Watchdog fires, marks run FAILED, slot freed, alert sent |
| FR-15 | Topological resume after root fix | Fix root failure with 3-hop dependent chain | DAGs resume hop-by-hop: hop=1 first, then hop=2, then hop=3 |
| FR-16 | Outcome written to knowledge graph after resolution | Complete end-to-end resolution cycle | Neo4j updated with incident, root cause, resolution, outcome, confidence |
| FR-17 | R3/R4 failures (auth/cert) always ops-escalated, never auto-fixed | Trigger auth failure (401), cert failure | Routes to R3/R4; immediate ops escalation with runbook link; zero auto-fix attempted |

---

## 4. Non-Functional Requirements

### Availability & Recovery

| Metric | Target | Notes |
|--------|--------|-------|
| **RTO** | 15 minutes | Time from failure detection to remediation initiated (auto path) |
| **RPO** | 0 minutes | Bronze layer immutability means any pipeline failure can be replayed from source — no data loss |
| **Heal Server availability** | 99.9% | FastAPI service on WCNP; liveness/readiness probes; auto-restart on crash |
| **Postgres session store** | 99.9% | Atomic CAS semantics; survives process restarts; recovery sessions durable |
| **Cascade containment SLA** | < 5 seconds | From failure detection to all downstream DAGs paused |

### Performance

| Metric | Target |
|--------|--------|
| Log pre-processor latency | < 1 second |
| Regex/embedding classification latency | < 500ms |
| Gemini RCA end-to-end (including tool calls) | < 3 minutes |
| PR creation latency after sandbox pass | < 2 minutes |
| Watchdog cycle | Every 15 minutes |

### Scalability

| Dimension | Target |
|-----------|--------|
| Pipeline failures handled concurrently | Up to 50 (debounced to root cause per DAG run) |
| Knowledge graph incidents | Scales to millions of records (Neo4j native) |
| Embedding index size | No hard limit (Vertex AI Vector Search or Pinecone) |
| LLM token cost per incident (Gemini) | < $0.05 average (most incidents resolved by tiers 1–3, not LLM) |

### Regulatory Compliance

| Standard | Applicability |
|----------|--------------|
| **SOX** | ✅ Required — Finance pipeline data feeds SOX-audited financial reports. Audit trail of all automated changes (PR history, knowledge graph, approval timestamps) is mandatory. |
| **PCI DSS** | ✅ Required — Payment transaction data flows through these pipelines. Generated code and config changes must not introduce PCI scope expansion. |
| **GDPR / CCPA** | ✅ Required — Order and customer data processed. No PII in log extracts sent to LLM. Pre-processor must strip PII before extract is passed to Gemini. |

### Resiliency

- **Deterministic layers first:** Layers 1–4 (idempotent operators, retry-with-cleanup, watchdog, escalation) handle 80% of failures without AI involvement — AI layer failure does not cause pipeline layer failure
- **Circuit breaker:** Max 3 auto-attempts per DAG/bucket per 24h — no infinite loop
- **Idempotency gate:** WRITE_APPEND BQ tasks never auto-retried — must use WRITE_TRUNCATE or MERGE to be eligible for auto-retry
- **Human approval gate:** No AI-generated change reaches production without engineer sign-off on PR

### Observability (Golden Signals)

| Signal | Metric | Alert threshold |
|--------|--------|----------------|
| **Latency** | Cascade containment time | > 10 seconds |
| **Traffic** | Incidents processed per hour | Spike > 3× baseline |
| **Errors** | Failed Heal Server requests | > 1% error rate |
| **Saturation** | Postgres session store queue depth | > 50 open sessions |
| **AI cost** | LLM tokens consumed per day | > $10/day |
| **Quality** | Auto-heal rate | < 40% for 24h (SLA breach) |

---

## 5. Current Landscape

### Existing pipelines

| System | Tech | Failure rate | Manual effort |
|--------|------|-------------|---------------|
| Finance-eComm FDL_CoreFinance | Airflow YAML DAGs + Dataproc | ~9 failures/day | 800+ hours/year |
| Sam's Club finance-neptune-airflow | Airflow Python DAGs + Databricks | Similar scale | Included in above |

### Current failure handling (as-is)

- `default_args.retries: 0` on most DAGs — no automatic retry
- No downstream containment — dependent DAGs continue running on stale data
- Manual log triage by engineers via Airflow UI + GCP Stackdriver
- Alert delivery via Outlook email to `GECGISFI28@email.wal-mart.com`
- No incident history or knowledge reuse — every failure triaged from scratch

### Tech debt being addressed

- Hardcoded cluster names causing Delete Cluster 404 failures (fixed by idempotent operators)
- `max_active_runs: 1` without zombie detection (fixed by external watchdog)
- `retries: 0` across all DAGs (corrected to 3 with exponential backoff + cleanup callback)
- No structured failure classification (addressed by 4-tier classification system)
- No incident history (addressed by Neo4j knowledge graph with feedback loop)

---

## 6. Solution — High-Level E2E Summary

The platform introduces **8 layers** organized into two tiers:

**Tier 1 — Deterministic self-healing (Layers 1–4, in-DAG):**
Handles ~80% of failures with no AI cost. Idempotent operators, retry-with-cleanup callbacks, external watchdog for zombie runs, and escalation to Pub/Sub when retries exhaust.

**Tier 2 — AI-assisted diagnosis and remediation (Layers 5–8):**
Activates only after deterministic layers fail. Log pre-processor compresses 100K+ line logs to 2–5K char extract. 4-tier classification (regex → fuzzy → embedding → Gemini) determines failure bucket. AI orchestrator queries Neo4j knowledge graph for historical resolutions. Gemini performs agentic RCA for novel failures using bounded context + MCP tool calls. Confidence-scored remediation routed to: auto-execute, PR + engineer review, or ops escalation.

**Human approval gate is mandatory** for all code/config changes before production deployment.

---

## 7. Technology Choice

### Options Considered

| Option | Description | Ruled out because |
|--------|-------------|------------------|
| **Pure rule-based** | Regex + Airflow retry only | Cannot handle novel failures; no learning over time |
| **LLM-first (send logs to LLM every failure)** | Send raw logs to GPT/Gemini on every failure | Cost: ~$0.05–0.10/failure × 9/day × 365 = $160–$330/year minimum; latency; hallucination risk on 100K-line inputs |
| **Single vector store (Pinecone/ChromaDB)** | Flat vector index for incident history | Cannot express relational structure (DAG lineage, incident→resolution→sandbox chains) needed for blast radius analysis |
| **Fine-tuned classifier (BERT/DistilBERT)** | Train a neural text classifier | Requires 100+ examples per class, GPU infra; overkill for 9 error buckets |

### Chosen Approach and Rationale

| Layer | Technology | Rationale |
|-------|-----------|-----------|
| Orchestration | Apache Airflow (existing) | Already in use; on_failure_callback + on_success_callback hooks enable zero-code integration |
| Heal Server | FastAPI on WCNP | Lightweight, async, WCNP-native deployment |
| Session state | PostgreSQL | CAS semantics for atomic state transitions; survives process restarts; not available in Airflow Variables |
| Log extraction | Regex pre-processor | <1 second, zero cost, reduces LLM input by 95% |
| Classification (v0) | Regex/signature rules | Ships day 1 with zero training data |
| Classification (v1) | Embedding + cosine similarity | Handles semantic variants (3 OOM phrasings → same embedding cluster); uses same model as knowledge graph (no impedance mismatch); few-shot (20 examples/bucket) |
| Knowledge graph | Neo4j | Stores relational incident structure (Incident → Resolution → SandboxRun, DAG lineage); graph traversal for blast radius analysis — not possible with flat vector index |
| LLM (RCA) | Gemini 2.5 Pro (complex) / Flash (infra) | Agentic tool-calling capability; structured output (Pydantic validation); only invoked when tiers 1–3 fail |
| Sandbox testing | PySpark local mode (unit) + Cloud Build staging (integration) | Tier 1 unit tests are free and run in <2 min; Tier 2 integration only for schema/Hudi changes |
| PR creation | GitHub API (existing GEC GitHub) | Existing approval workflow; PR history = audit trail for SOX |
| Security scanning | Snyk (existing Walmart integration) | Existing enterprise license; blocking gate before PR creation |

---

## 8. Data Flow Diagrams

### Failure detection and cascade containment

```
Pipeline task fails
        │
        ▼ on_failure_callback (< 1 sec)
FastAPI Heal Server (/pause endpoint)
        │
        ▼
FailurePropagator
  BFS traversal of Neo4j lineage graph
  Identifies all downstream dependent DAGs
        │
        ▼ Airflow REST API (parallel calls)
All downstream DAGs paused
Postgres: session record created (PAUSED state)
        │
        ▼
Pub/Sub event emitted → Log Pre-Processor
```

### Classification and remediation routing

```
Raw logs (GCS/Stackdriver)
        │
        ▼ Log Pre-Processor (<1 sec)
2–5K char bounded extract
        │
        ▼
Tier 1: Regex classifier → match? → route to remediation
        │ no match
        ▼
Tier 2: Fuzzy match vs Neo4j history → match? → route
        │ no match
        ▼
Tier 3: Embedding similarity → confidence ≥ 0.90? → route
  (weighted vote + label agreement + remediation divergence)
        │ confidence < 0.70
        ▼
Tier 4: Gemini agentic RCA
  Small initial prompt + MCP tool calls
  Pydantic-structured response
  evidence_sufficient gate
        │
        ▼
Confidence-based routing:
  ≥ 0.90 → auto-execute (retry / config change)
  0.70–0.90 → advisory alert only
  < 0.70 → ops escalation
```

### Code fix path (PR creation)

```
Gemini proposes code fix
        │
        ▼
Sandbox: pyspark local unit tests
        │ pass
        ▼
Snyk security scan + secret detection
        │ pass
        ▼
GitHub PR created (bot account, limited scope)
Engineer notified (auto-assigned reviewer)
        │
        ▼ Engineer approves and merges
Production deployment via existing CI/CD
        │
        ▼ on_success_callback fires
Heal Server: /check-and-resume
Postgres: PAUSED → RESUMING (atomic CAS)
DAGs resumed hop-by-hop (topological order)
Postgres: COMPLETE
Neo4j: outcome written back
```

---

## 9. Data Model

### Neo4j Knowledge Graph nodes and relationships

```
(Incident)
  properties: incident_id, timestamp, dag_id, task_id, environment,
              bucket (R1–R9), log_extract (2–5K chars), classifier_confidence

(RootCause)
  properties: root_cause_type, description, supporting_evidence[]

(Resolution)
  properties: resolution_type, proposed_fix, fix_diff, pr_url

(SandboxRun)
  properties: unit_tests_passed, integration_tests_passed, snyk_passed,
              secret_clean, confidence_score

(Runbook)
  properties: title, steps, bucket, validated_by_engineer, last_updated

(DAG)
  properties: dag_id, schedule, cluster_name, environment

Relationships:
  (Incident) -[:AFFECTS]→ (DAG)
  (Incident) -[:CAUSED_BY]→ (RootCause)
  (Incident) -[:RESOLVED_BY]→ (Resolution)
  (Resolution) -[:VALIDATED_IN]→ (SandboxRun)
  (DAG) -[:DEPENDS_ON {hop_depth: int}]→ (DAG)
  (DAG) -[:READS_FROM]→ (Dataset)
  (DAG) -[:WRITES_TO]→ (Dataset)
  (Runbook) -[:APPLIES_TO]→ (RootCause)
  (RootCause) -[:SIMILAR_TO {similarity: float}]→ (RootCause)
```

### Postgres session store (dag_recovery_sessions)

| Column | Type | Description |
|--------|------|-------------|
| session_id | UUID | Primary key |
| root_dag_id | String | The failed DAG that triggered the session |
| root_run_id | String | Airflow run ID |
| state | Enum | PAUSED / RESUMING / COMPLETE / RESUME_FAILED / STALE |
| paused_dags | JSON | List of paused downstream dag_ids with hop_depth |
| created_ts | Timestamp | When session was created |
| updated_ts | Timestamp | Last state transition |
| stale_alert_sent | Boolean | True after 24h escalation alert sent |

---

## 10. Contract & System Changes

| System | Change | Impact |
|--------|--------|--------|
| Airflow DAG default_args | Add `on_failure_callback`, `on_retry_callback`, `on_success_callback`; change `retries: 0 → 3`; add exponential backoff | All DAGs require update; backward-compatible |
| Airflow REST API | Called by Heal Server to pause/resume DAGs and update task state | No changes to Airflow API; only consumer changes |
| Dataproc cluster operators | Replace `DataprocDeleteClusterOperator` with idempotent wrapper; add `trigger_rule=all_done` on delete tasks | Per-DAG change; no schema changes |
| GCS/Stackdriver | Log pre-processor reads task logs via GCS API | Read-only access; no changes to logging infrastructure |
| GitHub (GEC) | Bot account (`svc-self-heal-bot`) creates PRs on `self-heal/*` branches | New service account; limited to branch creation + PR; cannot merge |
| Neo4j | New deployment on WCNP | Net-new component; no existing system changes |
| Postgres | New `dag_recovery_sessions` table | Net-new; dedicated instance or schema on existing Postgres |
| Pub/Sub | New topic for failure events | Net-new topic; DAG callbacks write to it |

---

## 11. Failure Modes

| Failure Mode | Impact | Mitigation |
|--------------|--------|-----------|
| Heal Server down | on_failure_callback POST fails; no cascade containment | Airflow callback failure doesn't fail the task; Watchdog still runs every 15 min as fallback; Heal Server on WCNP with auto-restart |
| Postgres unavailable | Session state lost; resume ordering may fail | Session recovery on restart; Watchdog detects PAUSED sessions > 24h and alerts |
| Gemini API unavailable or rate-limited | Tier 4 RCA unavailable | Falls back to ops escalation — same outcome as low-confidence case; no data pipeline impact |
| Neo4j unavailable | Knowledge graph queries fail | Falls back to tier 1/2 (regex/fuzzy) which don't require Neo4j; no pipeline impact |
| Bot account PR creation fails | Fix proposed but PR not created | Engineer notified with proposed diff; can create PR manually; no data pipeline impact |
| Snyk scan false positive | Blocks valid PR | Engineer receives scan result; can override with documented justification |
| Auto-retry makes data worse (WRITE_APPEND) | Duplicate rows in BQ | Idempotency gate blocks auto-retry for WRITE_APPEND tasks; only WRITE_TRUNCATE/MERGE eligible |
| Circuit breaker false positive (opens on recoverable failures) | Auto-healing suspended for DAG | Circuit opens after 3 attempts; engineer escalated with context; short CIRCUIT_OPEN_HOURS window before reset |
| Gemini hallucinated root cause (evidence_sufficient=TRUE but wrong) | Wrong PR created | Sandbox tests catch logic errors; Snyk catches security issues; engineer reviews before merge; human gate is final safety |
| PII in log extract sent to Gemini | Data privacy violation | Pre-processor must strip PII fields before extract passed to LLM; validated in testing |

---

## 12. Tech Debt

| New tech debt introduced | Plan to address | Milestone |
|--------------------------|----------------|-----------|
| Regex/signature library will grow organically and become hard to maintain | Migrate known patterns to embedding index (v1) as training data accumulates | 6 months post-launch |
| Neo4j is a new infrastructure dependency | Evaluate whether WCNP-managed graph database or managed cloud option (AuraDB) is more cost-effective at scale | 3 months post-launch |
| Bot account credentials (GEC GitHub PAT for PR creation) | Migrate to WCNP Workload Identity or short-lived token pattern when available | Next security review cycle |
| Sandbox pyspark local mode doesn't cover all pipeline patterns (e.g., GCS path resolution) | Add integration test tier (Cloud Build + staging) for high-risk fix types | Phase 2 |

---

## 13. Multi-Tenancy Certification

| Dimension | Status | Notes |
|-----------|--------|-------|
| **Multi-market support** | ✅ | Finance-eComm (US Walmart.com) + Sam's Club — separate DAG namespaces, separate knowledge graph partitions |
| **Multi-environment** | ✅ | Dev / staging / production isolation via Airflow environment variables; Heal Server deployed per environment |
| **Feature flags** | ✅ | `self_heal_mode` Airflow Variable controls shadow / supervised / full mode per environment |
| **Market-level circuit breakers** | ✅ | Circuit breaker scoped per `dag_id` — one market's failures don't suppress healing in another |
| **Configurable by policy** | ✅ | Max auto-attempts, confidence thresholds, escalation recipients all configurable via Airflow Variables — not hardcoded |

---

## 14. Discovery Track

| Open Question | Owner | Status |
|---------------|-------|--------|
| Does Gemini 2.5 Pro meet Walmart's data residency requirements for log data? | Security / Arch | Open |
| Is Neo4j approved in Walmart's GTP technology list, or does an exception need to be filed? | Architecture | Open |
| Should bot account (`svc-self-heal-bot`) live under Finance-eComm org or a shared platform org on GEC GitHub? | Platform / Security | Open |
| What is the approved Walmart pattern for short-lived GitHub tokens in automated systems? | Security | Open |
| For SOX audit trail — is the GitHub PR history sufficient, or does a separate immutable audit log need to be maintained? | Finance Compliance | Open |
| PII stripping in log pre-processor — which fields are in scope? (order_id, customer_id, address?) | Data Privacy / Legal | Open |

---

## 15. Operations & Cost

### Infrastructure cost estimate

| Component | Cost model | Estimated monthly |
|-----------|-----------|-------------------|
| FastAPI Heal Server (WCNP) | 2 replicas, always-on | ~$150/month |
| Postgres (session store) | Small managed instance | ~$100/month |
| Neo4j (knowledge graph) | WCNP-deployed or AuraDB | ~$300–500/month |
| Embedding API (text-embedding-004) | Per incident classified (tier 3) | ~$20/month (most incidents resolve at tiers 1–2) |
| Gemini 2.5 Pro (RCA) | Per token, only tier 4 failures | ~$50–100/month (est. 10–20% of failures reach Gemini) |
| Gemini 2.5 Flash (infra failures) | Per token | ~$10–20/month |
| Cloud Build (sandbox integration tests) | Per build (tier 2 sandbox only) | ~$30–50/month |
| **Total estimate** | | **~$700–950/month** |

**ROI comparison:** 720 engineering hours saved × fully-loaded eng cost → payback in < 1 month.

### Performance and load testing plan

- **Load test:** Simulate 50 concurrent DAG failures; verify Heal Server handles without queuing; cascade containment stays < 10 seconds
- **Embedding latency test:** Classify 1000 incidents via embedding similarity; verify p99 < 500ms
- **Gemini concurrency test:** Simulate 5 concurrent Gemini RCA calls; verify no rate limit hits; verify structured response schema validation passes
- **Chaos test (circuit breaker):** Trigger same failure 4× in 24h; verify circuit opens and escalation fires correctly

---

## 16. Security & SSP

### Security measures

| Layer | Control |
|-------|---------|
| **Heal Server** | WCNP internal network only; not exposed to internet; Walmart SSO for admin endpoints |
| **Airflow REST API** | Called from Heal Server using service account token; scoped to specific DAG operations only |
| **Bot account (GEC GitHub)** | Dedicated `svc-self-heal-bot` account; scoped to: create branch, create PR, read files; cannot approve or merge; branch protection rules prevent direct push to main |
| **Gemini API** | Called from WCNP private network; no PII in requests; log extract pre-screened for PII before passing to LLM |
| **Generated code (PRs)** | Snyk scan (high/critical CVE blocks PR); secret detection scan (credentials in diff blocks PR); human engineer reviews before merge |
| **Neo4j** | WCNP internal; not internet-facing; access via service account |
| **Postgres** | WCNP internal; credentials in WCNP secrets manager |

### SSP Requirements

An SSP must be filed covering:
1. Scope: WCNP-hosted (Heal Server, Neo4j, Postgres); GCP (log reads via existing service accounts); external LLM API (Gemini — data residency confirmation required)
2. New components to be clearly marked in architecture diagram: Heal Server, Neo4j, Postgres session store, bot GitHub account
3. Data flowing to Gemini: pre-processed log extract (PII-stripped) — no raw order data, no PII, no credentials
4. Authentication: Heal Server → Airflow REST API via SA token; Heal Server → GEC GitHub via bot PAT (to be replaced with short-lived token)
5. All network connections to be documented with direction and port

> **APM ID must be obtained before SSP submission.**

---

## 17. Delivery

### Phase approach

| Phase | Scope | Gate criteria |
|-------|-------|--------------|
| **Phase 0** | Deterministic layers: idempotent operators, retry-with-cleanup, watchdog | All FR-1 through FR-4, FR-9, FR-14 pass; zero regressions in existing pipeline tests |
| **Phase 1** | 4-tier classification, Neo4j knowledge graph, shadow mode (observe only) | All FR-5 through FR-8, FR-16, FR-17 pass; 30-day shadow period with accuracy reporting |
| **Phase 2** | Auto-retry (R1/R5/R7), PR creation for code fixes | FR-10 through FR-13, FR-15 pass; confidence thresholds calibrated on shadow data |
| **Phase 3** | Full autonomous mode, feedback loop, circuit breaker | FR-16, FR-17, all NFRs validated; auto-heal rate ≥ 40% before enabling full mode |

### LOE estimate (T-shirt sizing)

| Phase | Engineering effort |
|-------|--------------------|
| Phase 0 (deterministic) | M — 2–3 sprints |
| Phase 1 (classification + shadow) | L — 4–5 sprints |
| Phase 2 (auto-execute + PR) | L — 4–5 sprints |
| Phase 3 (full + calibration) | M — 2–3 sprints |

---

## 18. Requirement Traceability Matrix

| Business Requirement | Solution Component | FR |
|---------------------|-------------------|-----|
| Eliminate cascading data corruption | BFS downstream pause (Heal Server + FailurePropagator) | FR-2 |
| Reduce manual triage to < 5 min | Log pre-processor + 4-tier classification | FR-3, FR-4, FR-5 |
| Auto-resolve known failures without engineer | Deterministic layers + auto-retry with parameter change | FR-1, FR-9 |
| Human approval before production changes | GitHub PR + mandatory engineer review | FR-12 |
| Prevent wrong remediation from similar failures | Label agreement + remediation divergence penalty on P_vector | FR-6 |
| Prevent hallucinated root causes | evidence_sufficient gate + Pydantic schema + sandbox validation | FR-7, FR-8, FR-10 |
| SOX audit trail | GitHub PR history + Neo4j incident log + Postgres session log | FR-16 |
| Never auto-fix auth/cert failures | R3/R4 hard escalation route | FR-17 |
| Continuous improvement over time | Feedback loop: outcomes written to Neo4j, confidence calibrated | FR-16 |
| Cost control on AI usage | 4-tier classification — LLM only for tier 4 | FR-4, FR-5, FR-6 |

---

## 19. Checklist

| # | Artifact | Status |
|---|----------|--------|
| 1 | Product Requirements | ✅ Sections 1–2 |
| 2 | ROI & KPI | ✅ Section 1 |
| 3 | Business Use-Cases (testable) | ✅ Section 2 |
| 4 | Non-Functional Requirements | ✅ Section 4 |
| 5 | Current Landscape | ✅ Section 5 |
| 6 | E2E Solution Summary | ✅ Section 6 |
| 7 | Conceptual Data Model | ✅ Section 9 |
| 8 | Failure Modes | ✅ Section 11 |
| 9 | Detailed Solution reference links | ✅ AI_Based_Self_Healing.md, DATA_ENGINEERING_PLATFORM.md |
| 10 | Discovery Track | ✅ Section 14 |
| 11 | Multi-Tenancy Certification | ✅ Section 13 |
| 12 | Delivery details | ✅ Section 17 |

### Outstanding before ARB submission

- [ ] APM ID obtained
- [ ] SSP filed and SSP# referenced in Section 16
- [ ] Gemini data residency confirmed with Security
- [ ] Neo4j GTP approval confirmed or exception filed
- [ ] PII field list for log pre-processor confirmed with Data Privacy
- [ ] Architecture diagram (draw.io) attached with network boundaries, ports, all new components bolded
- [ ] NFR document filed at DX NFR Template link
- [ ] Discovery Track open questions resolved or owners assigned
- [ ] LOE reviewed and signed off by Tech Lead and PM

---

*ARB Template: [EDEE Architecture Design Template](https://confluence.walmart.com/display/EDEE/Architecture+Design+Template)*
*ARB Process: [Architecture Review Process](https://dx.walmart.com/guides/dx/Architecture-Review-Process-D0000002242)*
*NFR Template: [DX NFR Template](https://dx.walmart.com/guides/dx/Non-Functional-Requirements-Template-D8j1fr48j52)*
