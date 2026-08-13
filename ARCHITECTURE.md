# Self-Healing AI Pipeline Architecture
### Airflow + Dataproc + GCP — Finance-eComm & Sam's Club Finance

> **Design principle:** Cheap, deterministic layers handle 80% of failures instantly.
> The AI brain activates only for novel or low-confidence cases — never burning tokens on known, cheap fixes.

> **Honest expectation:** R3 (auth) and R4 (cert) — historically your highest-frequency real failures (expired PATs,
> revoked SA keys) — are **not auto-fixable**. They require secret rotation by a human. This system targets
> ~55–65% auto-resolution rate on the remaining buckets. Don't size this as a 100% replacement for on-call.

---

## Table of Contents

1. [Failure Taxonomy](#1-failure-taxonomy)
2. [Overall Architecture](#2-overall-architecture)
3. [Layers 1–4: Deterministic Self-Healing](#3-layers-14-deterministic-self-healing)
4. [Layer 5: ML Classifier](#4-layer-5-ml-classifier)
5. [Layer 6: AI Orchestrator + Vector Memory](#5-layer-6-ai-orchestrator--vector-memory)
6. [Layer 7: Resolution Executor](#6-layer-7-resolution-executor)
7. [Layer 8: PR Factory + Security Gate](#7-layer-8-pr-factory--security-gate)
8. [Confidence Score Definition](#8-confidence-score-definition)
9. [Cascading Failure Debounce](#9-cascading-failure-debounce)
10. [Observability & Cost Tracking](#10-observability--cost-tracking)
11. [Cold-Start & Shadow Mode](#11-cold-start--shadow-mode)
12. [Circuit Breaker](#12-circuit-breaker)
13. [Feedback Loop](#13-feedback-loop)
14. [Implementation Roadmap](#14-implementation-roadmap)

---

## 1. Failure Taxonomy

Every pipeline failure maps to one of 9 buckets. The bucket determines the **resolution path**, not just the fix.

| # | Bucket | Examples | Resolution Path |
|---|--------|----------|-----------------|
| **R1** | Resource exhaustion | OOM, CPU throttle, disk full, Dataproc quota exceeded | Auto-retry with scaled config |
| **R2** | Dependency mismatch | Package version conflict, missing JAR, deprecated API, PySpark incompatibility | Code/config PR |
| **R3** | Auth / credential failure | Token expired (PAT, SA key), IAM role revoked, Airflow connection invalid | **Ops escalate** — no auto-fix |
| **R4** | Certificate / TLS failure | Cert expired, self-signed cert rejected, Walmart proxy cert change | **Ops escalate** — no auto-fix |
| **R5** | Network / connectivity | Timeout, DNS failure, VPC firewall block, GCS unreachable | Auto-retry with backoff |
| **R6** | Data / schema failure | Schema mismatch, null constraint, corrupt input, missing partition, Hudi conflict | Code/config PR (after sandbox) |
| **R7** | Cluster / infra failure | Cluster NOT_FOUND, cluster in ERROR state, preempted nodes, regional GCP degradation | Auto-retry (idempotent ops) or Ops escalate for regional outage |
| **R8** | DAG / code failure | Python import error, syntax error, UDF failure, logic bug | Code PR (after sandbox) |
| **R9** | External dependency | Upstream DAG stuck, touch file missing, BQ table absent, Anaplan timeout | Wait-and-retry / escalate upstream |

> **R3 and R4 are never auto-fixed.** Token/cert rotation requires human action (secret manager, GCP cert authority, IT ticket). Routing them to a code sandbox wastes time and creates false confidence.

---

## 2. Overall Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         PIPELINE FAILURE EVENT                               │
│             (Airflow on_failure_callback → Pub/Sub topic)                   │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │  {dag_id, task_id, run_id, error_msg, log_url}
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  LOG PRE-PROCESSOR  (Stage 1 — always runs, <1 sec, zero LLM cost)           │
│  • Pull full logs from GCS/Stackdriver  (100K+ lines raw)                    │
│  • Regex extracts: critical error section, file candidates, line numbers,    │
│    stack trace, last meaningful exception — reduces to 2–5K chars            │
│  • Raw logs are NEVER sent to LLM or classifier — only the extracted section │
│  • Output: {error_section, file_candidates, line_numbers, log_extract}       │
└──────────────────────────────────────────────────────────────────────────────┘
                               │  2–5K chars (not 100K)
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  LAYER 5 — ML Classifier (Stage 2)                                            │
│  • Extract 50+ structured features from compressed log_extract               │
│  • v0: Regex rules classify R1–R9 bucket (<1 sec, $0)                        │
│  • v1: XGBoost classifies R1–R9 bucket  (<100ms, no LLM cost)               │
│  • Output: {bucket, confidence_score, extracted_features}                    │
└──────────────────────────────────────────────────────────────────────────────┘
                               │
          ┌────────────────────┼────────────────────┐
          │ confidence > 0.85  │                     │ confidence < 0.85
          ▼                    │                     ▼
  Known issue,                 │          ┌──────────────────────────┐
  bucket confirmed             │          │  LLM Enricher (Haiku)    │
          │                    │          │  Re-classifies, extracts │
          │                    │          │  root cause summary       │
          │                    │          └──────────┬───────────────┘
          └────────────────────┴──────────────────── ┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  LAYER 6 — AI Orchestrator (2nd Brain)                                        │
│  Input: {bucket, features, error_summary}                                     │
│                                                                               │
│  ┌─────────────────────┐          ┌────────────────────────────────────────┐ │
│  │  Vector DB Query    │          │  Web Search Agent (if no match)        │ │
│  │  Pinecone / Vertex  │          │  • Claude Opus: complex/novel issues   │ │
│  │  AI Vector Search   │          │  • Claude Haiku: known error patterns  │ │
│  │                     │          │  • Walmart internal Stack Overflow     │ │
│  │  similarity_score,  │          │  • GitHub issues, GCP docs             │ │
│  │  past_resolution,   │◄────────►│                                        │ │
│  │  success_rate       │          └────────────────────────────────────────┘ │
│  └─────────────────────┘                                                      │
│                                                                               │
│  Output: {resolution_type, proposed_fix, confidence_score}                   │
└──────────────────────────────────────────────────────────────────────────────┘
                               │
          ┌────────────────────┼──────────────────────────────┐
          │                    │                              │
          ▼                    ▼                              ▼
   RESOLUTION TYPE A    RESOLUTION TYPE B/C           RESOLUTION TYPE D
   Auto-retry /         Code / Config / Package        Ops Escalate
   Config toggle        change → Sandbox               (R3: auth, R4: cert,
                                                        regional outage)
          │                    │
          ▼                    ▼
   Auto-execute         Sandbox test env
   record outcome       Unit tests pass?
                              │
                    ┌─────────┴────────┐
                    │ Yes + conf>90%   │ No / conf<90%
                    ▼                  ▼
             LAYER 8:           Alert developer,
             PR Factory         attach test failure
             + Snyk scan        report, no PR
                    │
                    ▼
             PR created →
             Developer reviews →
             (gatekeeper merges)
```

---

## 3. Layers 1–4: Deterministic Self-Healing

These run **inside the DAG itself** — no AI cost, fixes ~80% of transient failures immediately.

### Layer 1: Idempotent Operators
```python
# plugins/safe_dataproc_ops.py

def safe_delete_cluster(project_id, region, cluster_name, **context):
    """Delete Dataproc cluster; treat NotFound as success."""
    try:
        op = client.delete_cluster(request={...})
        op.result(timeout=300)
    except gcp_exceptions.NotFound:
        logging.info(f"Cluster {cluster_name} already gone — skipping. ✓")
    except gcp_exceptions.FailedPrecondition:
        logging.warning("Already being deleted — skipping.")


def safe_create_cluster(project_id, region, cluster_name, cluster_config, **context):
    """Create cluster; if RUNNING already exists, reuse."""
    try:
        op = client.create_cluster(request={...})
        op.result(timeout=600)
    except gcp_exceptions.AlreadyExists:
        cluster = client.get_cluster(...)
        if cluster.status.state.name == "RUNNING":
            logging.info("Cluster already RUNNING — reusing. ✓")
        else:
            raise RuntimeError(f"Cluster in {cluster.status.state.name} — needs manual cleanup.")

# Wire: trigger_rule='all_done' on delete task — runs even if upstream fails
```

### Layer 2: Retry-with-Cleanup
```python
# default_args — every DAG
default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'on_retry_callback': cleanup_before_retry,   # tears down broken state first
    'on_failure_callback': emit_failure_event,   # → Pub/Sub → AI brain
}

def cleanup_before_retry(context):
    if 'create_cluster' in context['task_instance'].task_id:
        _force_delete_orphan_cluster(context)
    if 'wait_touch' in context['task_instance'].task_id:
        _log_touch_file_status(context)
    context['task_instance'].clear_xcom_data()
```

### Layer 3: External Watchdog
Cloud Scheduler (every 15 min) → Cloud Function → Airflow REST API.
Detects zombie runs (age > 6h) → marks failed → unblocks `max_active_runs: 1`.

### Layer 4: Escalation
`on_failure_callback` after all retries exhausted → emits to Pub/Sub → triggers AI brain (Layer 5+).

---

## 4. Layer 5: ML Classifier

### Stage 1 — Log Pre-Processor (always runs first, before any classifier or LLM)

Raw Airflow/Dataproc logs are 100K+ lines. Sending them raw to an LLM or classifier is:
- **Slow** — LLM context windows fill up, latency spikes
- **Expensive** — tokens cost money; 100K lines = ~75K tokens per failure
- **Noisy** — Spark driver logs contain verbose GC output, task progress, shuffle stats that obscure the actual error

**The fix (sourced from SIAGI Self-Healing Agent pattern):** A dedicated regex pre-processor runs first, in <1 second, at zero LLM cost. It extracts only the signal:

| What's extracted | How | Why |
|-----------------|-----|-----|
| Critical error section | Regex: last `Exception`, `Error`, `FAILED` block | The root cause, not the noise |
| File candidates | Regex: paths matching `*.py`, `*.sql`, `*.yaml` near the error | Tells LLM which file to patch |
| Line numbers | Regex: `line \d+`, `at .*:\d+` near the exception | Enables line-level patching |
| Stack trace | Regex: indented `at ...` block | Context for root cause analysis |
| Exit code | Regex: `exit code \d+`, `returncode=\d+` | Immediate bucket signal (137 = OOM) |

**Output:** `{error_section, file_candidates, line_numbers, stack_trace, exit_code}` — typically 2–5K chars.

**Rule: raw logs never reach the LLM or classifier.** Only the pre-processed extract is passed downstream.

---

### Classifier evolution — ship v0 first, earn XGBoost

**v0 (ship immediately — no training data needed):** The regex patterns in `FEATURE_SCHEMA` below already
*are* a bucket classifier. Your `has_auth`, `has_cert`, `has_oom` patterns are mutually exclusive enough to
route 70%+ of incidents correctly with zero ML. Ship this first. Collect labeled outcomes.

**v1 (after 200+ labeled incidents):** Train XGBoost on those labels. Until then, XGBoost adds complexity
with no accuracy gain over regexes on a cold dataset.

| Classifier version | When to use | Cold-start? |
|--------------------|------------|-------------|
| v0: Regex rules | Now, Day 0 | Works immediately |
| v1: XGBoost + TF-IDF | After 200 labeled incidents | Needs training data |

### Why XGBoost over LLM-first (for v1+)

| Criterion | XGBoost + TF-IDF | LLM-first |
|-----------|-----------------|-----------|
| Latency | <100ms | 2–10s |
| Cost | ~$0 | $0.01–0.10/call |
| Volume | Every failure | Only novel ones |
| Accuracy on known errors | 92–96% (trained on logs) | High but variable |
| Interpretable | Yes (SHAP values) | No |

**Decision: XGBoost classifies everything. LLM activates only when XGBoost confidence < 0.85.**

### Feature Extraction from Logs

```python
# classifier/feature_extractor.py

FEATURE_SCHEMA = {
    # Error signal features
    'has_oom':              r'(OutOfMemoryError|GC overhead|Killed|exit code 137)',
    'has_timeout':          r'(TimeoutError|DEADLINE_EXCEEDED|timed out)',
    'has_not_found':        r'(NOT_FOUND|404|no such file|does not exist)',
    'has_permission':       r'(PERMISSION_DENIED|403|Forbidden|Access Denied)',
    'has_auth':             r'(Unauthorized|401|invalid_token|token expired|Bad credentials)',
    'has_cert':             r'(SSLError|certificate|CERTIFICATE_VERIFY_FAILED|cert expired)',
    'has_package':          r'(ModuleNotFoundError|ImportError|version.*conflict|NoSuchMethodError)',
    'has_schema':           r'(SchemaException|AnalysisException|cannot resolve column|type mismatch)',
    'has_quota':            r'(RESOURCE_EXHAUSTED|quota exceeded|rateLimitExceeded)',
    'has_cluster':          r'(cluster not found|CLUSTER_NOT_FOUND|cluster.*ERROR state)',
    'has_network':          r'(ConnectionError|ConnectionRefused|DNS|unreachable|ECONNRESET)',
    'has_upstream':         r'(touch file|ETL_LOAD_PARAMETERS|external task|upstream)',
    # Context features
    'task_is_create':       lambda t: int('create' in t.lower()),
    'task_is_delete':       lambda t: int('delete' in t.lower()),
    'task_is_load':         lambda t: int('load' in t.lower() or 'hudi' in t.lower()),
    'task_is_sensor':       lambda t: int('sensor' in t.lower() or 'wait' in t.lower()),
    'retry_count':          lambda ctx: ctx.get('try_number', 1),
    'hour_of_day':          lambda ctx: ctx.get('execution_date').hour,
    # TF-IDF top-50 log terms (fitted at training time)
    'tfidf_features':       'vectorized from error_message + last 100 log lines',
}

TARGET_BUCKETS = ['R1_resource', 'R2_dependency', 'R3_auth', 'R4_cert',
                  'R5_network', 'R6_data', 'R7_cluster', 'R8_code', 'R9_upstream']
```

### Training & Serving

```python
# classifier/train.py
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler
from scipy.sparse import hstack
import xgboost as xgb

# Training data: labeled historical incidents from vector DB
# Label: bucket (R1–R9), features: structured + TF-IDF

model = xgb.XGBClassifier(
    n_estimators=300,
    max_depth=6,
    learning_rate=0.05,
    use_label_encoder=False,
    eval_metric='mlogloss',
    n_jobs=-1,
)

# Serving: deploy as Cloud Run container
# Input: {error_message, log_snippet, task_id, dag_id, retry_count}
# Output: {bucket, confidence, top_features}  — p99 latency < 50ms
```

### Model refresh
Retrain weekly on new labeled incidents from the vector DB feedback loop.
Use SHAP to explain predictions — helps developers trust and audit the classifier.

---

## 5. Layer 6: AI Orchestrator + Vector Memory

### Vector Database Schema

```python
# Pinecone / Vertex AI Vector Search / ChromaDB

INCIDENT_SCHEMA = {
    # Identity
    'incident_id':          str,    # UUID
    'timestamp':            datetime,
    'dag_id':               str,
    'task_id':              str,
    'run_id':               str,
    'environment':          str,    # dev/stg/prod

    # Classification
    'bucket':               str,    # R1–R9
    'classifier_confidence': float,
    'error_message':        str,
    'log_snippet':          str,    # pre-processed extract (2–5K chars, not raw 100K lines)

    # Resolution
    'resolution_type':      str,    # auto_retry | config_change | code_pr | ops_escalate
    'proposed_fix':         str,    # human-readable description
    'fix_diff':             str,    # actual code/config diff (if code_pr)
    'pr_url':               str,    # GitHub PR URL (if created)

    # Outcome (written back after resolution)
    'resolution_successful': bool,
    'time_to_resolve_mins':  int,
    'sandbox_passed':        bool,
    'snyk_passed':           bool,
    'confidence_score':      float,  # final composite score

    # Vector embedding (for similarity search)
    # Embed: error_message + log_snippet + bucket + task_id
    'embedding':            List[float],  # 768-dim, text-embedding-004 or ada-002
}
```

### Orchestrator Flow

```python
# orchestrator/brain.py

def route_to_resolution(event: FailureEvent) -> ResolutionPlan:
    # Step 1: Query vector DB for similar past incidents
    similar = vector_db.query(
        vector=embed(event.error_message + event.log_snippet),
        top_k=5,
        filter={'bucket': event.bucket}
    )

    if similar and similar[0]['score'] > 0.88:
        # HIGH SIMILARITY — propose resolution from history
        past = similar[0]
        return ResolutionPlan(
            source='vector_db',
            resolution_type=past['resolution_type'],
            proposed_fix=past['proposed_fix'],
            historical_success_rate=_success_rate(similar),
            vector_similarity=similar[0]['score'],
            model_used='none',  # no LLM needed
        )
    else:
        # LOW SIMILARITY or NOVEL — invoke LLM
        model = 'claude-opus-4' if event.bucket in ['R2_dependency', 'R8_code'] else 'claude-haiku-4'

        llm_analysis = llm_client.messages.create(
            model=model,
            messages=[{
                'role': 'user',
                'content': RESOLUTION_PROMPT.format(
                    bucket=event.bucket,
                    error=event.error_message,
                    logs=event.log_snippet,
                    similar_incidents=_format_similar(similar),
                    dag_context=event.dag_id + '/' + event.task_id,
                )
            }],
            max_tokens=2000,
        )

        return ResolutionPlan(
            source='llm_search',
            **_parse_llm_response(llm_analysis),
            model_used=model,
        )
```

### Model routing logic

| Condition | Model | Reason |
|-----------|-------|--------|
| Vector similarity > 0.88 (known issue) | **None** — use history | Zero cost |
| Novel issue, R2/R8 (code/dependency) | **Claude Haiku first → Opus if low confidence** | Haiku handles most patterns; Opus only for genuinely novel code reasoning |
| Novel issue, R1/R5/R7 (infra/network/resource) | **Claude Haiku** | Pattern-matching sufficient, high volume |
| R3/R4 (auth/cert) — any | **Haiku** for runbook lookup only | Never proposes auto-fix |
| Confidence still < 0.6 after LLM | **Ops escalate** | Don't guess on prod |

> **Internal knowledge first:** Before any web search, query Walmart internal sources via `content_search`
> (Wibey MCP) — internal Stack Overflow, Confluence, internal GitHub issues. These have higher signal-to-noise
> for Walmart-specific GCP configs, WCNP quirks, and pipeline patterns than public web results.
> Web search (external) is last resort only.

---

## 6. Layer 7: Resolution Executor

Four resolution paths — bucket determines path, confidence determines whether to execute.

### Path A: Parameter-Change + Retry
```
Buckets: R1 (OOM → scale up), R5 (network → retry), R7 (cluster → safe recreate), R9 (upstream → wait)
No code change, no sandbox needed.
```

> ⚠️ **Critical:** By the time Layer 4 escalates here, Layers 1–2 already retried 3×.
> A plain retry is a no-op — it replays the same failure. **Path A MUST change a parameter
> before retrying**, or it repeats exactly what the user reported: "auto-restart also failed."

```python
# Resolution actions indexed by bucket — each changes state before retrying
PATH_A_ACTIONS = {
    'R1_resource': [
        # OOM: scale worker memory before retry
        lambda e: Variable.set(f'{e.dag_id}_worker_memory',
                               _scale_up(Variable.get(f'{e.dag_id}_worker_memory', '8g'))),
        # quota: add 10-min wait before retry to let quota recover
        lambda e: time.sleep(600) if 'quota' in e.error_message.lower() else None,
    ],
    'R5_network': [
        # Flush any stuck connection pools — no-op in Airflow but forces new worker
        lambda e: airflow_api.clear_task(e.dag_id, e.run_id, e.task_id),
    ],
    'R7_cluster': [
        # Safe delete (idempotent) before allowing create to retry
        lambda e: safe_delete_cluster(e.project_id, e.region, e.cluster_name),
    ],
    'R9_upstream': [
        # Log current touch file state so retry has fresh info
        lambda e: _log_touch_file_status(e),
    ],
}

def execute_auto_retry(plan: ResolutionPlan, event: FailureEvent):
    # Apply state-change action for this bucket
    actions = PATH_A_ACTIONS.get(event.bucket, [])
    for action in actions:
        action(event)

    # Now retry — state is different, not a replay
    airflow_api.patch_task_instance(
        dag_id=event.dag_id,
        dag_run_id=event.run_id,
        task_id=event.task_id,
        new_state='up_for_retry',
    )
    record_outcome(event, plan, resolution_attempted=True)
```

### Path B: Code / Config / Package PR
```
Buckets: R2 (dependency), R6 (data/schema), R8 (code bug)
Must pass sandbox + Snyk before PR.
```

> **Sandbox cost reality:** "Run tests in staging" means spinning a Dataproc cluster ($15–30, 10–15 min).
> Do NOT do this for every auto-generated fix. Use this two-tier approach:
>
> - **Tier 1 (always):** `pyspark` local-mode unit tests. Run in <2 min, $0, catches 80% of code bugs.
> - **Tier 2 (only if Tier 1 passes):** Cloud Build integration test in staging env. Only for schema/Hudi changes where local-mode can't simulate GCS partitioning.

```python
def execute_code_fix(plan: ResolutionPlan, event: FailureEvent):
    # 1. Create isolated sandbox branch
    branch = f'self-heal/{event.dag_id}-{event.incident_id[:8]}'
    git_client.create_branch(base='main', new_branch=branch)

    # 2. Apply proposed diff
    git_client.apply_diff(branch, plan.fix_diff)

    # 3a. Tier 1: pyspark local-mode unit tests (fast, cheap — always run)
    unit_result = sandbox.run_unit_tests(
        branch=branch,
        test_script=f'tests/unit/test_{event.dag_id}.py',
        mode='local',        # pyspark local[4], no cluster needed
        timeout_mins=5,
    )

    if not unit_result.passed:
        notify_developer(event, pr_url=None, confidence=0,
                        failure_reason='Unit tests failed', test_result=unit_result)
        return  # Don't proceed to integration or PR

    # 3b. Tier 2: integration test only for schema/data changes (costs cluster)
    integration_result = None
    if event.bucket in ['R6_data'] or 'hudi' in event.task_id.lower():
        integration_result = sandbox.run_integration_tests(
            branch=branch,
            env='staging',
            timeout_mins=20,
        )

    # 4. Snyk security scan + secret detection (BLOCKING gates)
    snyk_result = snyk.scan(branch)

    # Secret scan: never allow bot to commit credentials into generated code
    # (self-healing session itself has pasted live PATs — the bot must not do the same)
    secret_scan = detect_secrets.scan_diff(plan.fix_diff)
    if secret_scan.has_secrets:
        escalate_to_ops(plan, event, reason='Generated code contains potential secrets — blocked')
        return

    # 5. Compute final confidence — only PR if > 90%
    confidence = compute_confidence(plan, test_result, snyk_result)

    if confidence >= 0.90 and test_result.passed and snyk_result.passed:
        pr_url = create_pr(branch, plan, event, confidence)
        notify_developer(event, pr_url, confidence)
    else:
        notify_developer(event, pr_url=None, confidence=confidence,
                        test_result=test_result, snyk_result=snyk_result)
        # Store failed attempt — helps calibrate future confidence
        record_outcome(event, plan, resolution_attempted=False,
                      failure_reason=f'confidence={confidence:.2f}')
```

### Path C: Ops Escalate (non-auto-fixable)
```
Buckets: R3 (auth), R4 (cert), regional GCP outage, any conf < 0.60
```
```python
def escalate_to_ops(plan: ResolutionPlan, event: FailureEvent):
    # Never attempt auto-fix for auth/cert/infra-wide failures
    RUNBOOK_URLS = {
        'R3_auth': 'https://confluence.walmart.com/display/.../token-rotation',
        'R4_cert': 'https://confluence.walmart.com/display/.../cert-renewal',
    }
    send_alert(
        to='GECGISFI28@email.wal-mart.com',
        subject=f'[ACTION REQUIRED] {event.bucket}: {event.dag_id}.{event.task_id}',
        body={
            'bucket': plan.bucket,
            'error': event.error_message,
            'proposed_action': plan.proposed_fix,  # human runbook steps
            'runbook': RUNBOOK_URLS.get(event.bucket, ''),
            'confidence': plan.confidence_score,
        }
    )
```

---

## 7. Layer 8: PR Factory + Security Gate

```python
# pr_factory/create_pr.py

def create_pr(branch: str, plan: ResolutionPlan, event: FailureEvent, confidence: float) -> str:
    pr = github_client.create_pull_request(
        repo=event.repo,
        head=branch,
        base='main',
        title=f'[Self-Heal] {event.bucket}: {event.dag_id}.{event.task_id}',
        body=PR_TEMPLATE.format(
            incident_id=event.incident_id,
            bucket=plan.bucket,
            error_summary=event.error_message[:500],
            proposed_fix=plan.proposed_fix,
            confidence=f'{confidence:.1%}',
            test_status='✅ Passed',
            snyk_status='✅ No vulnerabilities',
            vector_similarity=f'{plan.vector_similarity:.2f}',
            historical_success_rate=f'{plan.historical_success_rate:.1%}',
            source=plan.source,
            model_used=plan.model_used or 'N/A (vector DB match)',
        ),
        labels=['self-heal', f'bucket-{plan.bucket}', f'conf-{int(confidence*100)}'],
        reviewers=[event.dag_owner],   # auto-assign DAG owner as reviewer
    )

    # PR is NEVER auto-merged — developer is mandatory gatekeeper
    return pr.html_url

PR_TEMPLATE = """
## 🔧 Self-Heal PR

| Field | Value |
|-------|-------|
| **Incident ID** | {incident_id} |
| **Failure Bucket** | {bucket} |
| **Confidence Score** | {confidence} |
| **Historical Success Rate** | {historical_success_rate} |
| **Vector Similarity** | {vector_similarity} |
| **Source** | {source} |
| **Model Used** | {model_used} |

### Error Summary
```
{error_summary}
```

### Proposed Fix
{proposed_fix}

### Gate Results
- Tests: {test_status}
- Snyk Scan: {snyk_status}

---
⚠️ **This PR was generated by the self-healing system.**
A human engineer must review and approve before merge.
Auto-merge is disabled. If you believe the fix is incorrect, close this PR and file a manual incident.

*co-authored-by: wibey self-healing agent*
"""
```

### Snyk gate (blocking)

```yaml
# .github/workflows/self-heal-pr.yml
on:
  pull_request:
    branches: [main]
    paths-ignore: ['*.md']

jobs:
  snyk-scan:
    runs-on: ubuntu-latest
    steps:
      - uses: snyk/actions/python@master
        env:
          SNYK_TOKEN: ${{ secrets.SNYK_TOKEN }}
        with:
          args: --severity-threshold=high --fail-on=all
    # PR merge is blocked until this passes
```

---

## 8. Confidence Score Definition

### Why NOT a product formula

The original v1 of this doc used `P_classifier × P_vector × P_success_rate`. That's wrong for correlated
signals: when a failure is "known" (seen many times before), **both** P_classifier and P_vector will be high
together — they carry the same information. Multiplying them double-penalizes the best cases.

**Proof:** Known OOM seen 20× → P_cls=0.97, P_vec=0.95, P_success=0.92.
Product = 0.85 → below the 0.90 gate → auto-retry **never fires** on your most healable case. The formula was self-defeating.

### Correct formula: min() for probabilistic signals, hard gates for binary checks

```
confidence = min(P_classifier, P_vector, P_success_rate)

PLUS hard boolean gates (veto power — any failure blocks the path):
  sandbox_passed  : bool  — all unit tests passed  (required for code PR)
  snyk_passed     : bool  — no high/critical CVEs   (required for code PR)
  secret_clean    : bool  — no credentials in diff  (required for code PR)

Effective rule:
  For code/config PR:  confidence = min(...) AND sandbox_passed AND snyk_passed AND secret_clean
  For auto-retry:      confidence = min(P_classifier, P_success_rate)  [P_vector not applicable]

Where:
  P_classifier   = XGBoost (or regex-rules) probability for predicted bucket   [0.0 – 1.0]
  P_vector       = cosine similarity to best matching past incident             [0.0 – 1.0]
                   (= 0.50 if no past incident — cold start penalty)
  P_success_rate = historical success rate of this fix type                    [0.0 – 1.0]
                   (= 0.65 default if < 3 past incidents of this type)

Thresholds:
  confidence ≥ 0.90 AND all gates pass  → create PR  (or execute auto-retry)
  0.70 ≤ confidence < 0.90              → advisory alert only, no execution
  confidence < 0.70                     → ops escalate
```

```python
def compute_confidence(plan, unit_result=None, integration_result=None,
                       snyk_result=None, secret_result=None) -> float:
    # Probabilistic signals — min(), not product
    p_signals = [plan.classifier_confidence, plan.historical_success_rate]
    if plan.vector_similarity is not None:
        p_signals.append(plan.vector_similarity)

    confidence = min(p_signals)

    # Hard boolean gates — veto any execution
    if unit_result and not unit_result.passed:
        return 0.0   # Tests failed — never PR
    if snyk_result and not snyk_result.passed:
        return 0.0   # Security issue — never PR
    if secret_result and secret_result.has_secrets:
        return 0.0   # Credentials in generated code — never PR

    return confidence
```

### Corrected example calculations

| Scenario | P_cls | P_vec | P_success | min() | Gates | **Score** | Action |
|----------|-------|-------|-----------|-------|-------|-----------|--------|
| Known OOM, seen 20× before, retry works | 0.97 | 0.95 | 0.92 | **0.92** | N/A (retry) | **0.92** | ✅ Auto-retry |
| Package mismatch, similar fix, tests pass | 0.91 | 0.88 | 0.85 | **0.85** | All pass | **0.85** | Advisory only |
| Novel code bug, LLM proposes fix, tests pass | 0.78 | 0.55 | 0.70 | **0.55** | All pass | **0.55** | Ops escalate |
| Known schema fix, Snyk finds CVE | 0.95 | 0.91 | 0.90 | 0.90 | Snyk=FAIL | **0.0** | Blocked — Snyk |
| Cold start (no vector history) | 0.91 | 0.50 | 0.65 | **0.50** | All pass | **0.50** | Ops escalate |

> **Calibration:** Use isotonic regression on XGBoost output probabilities to get calibrated scores.
> Start all thresholds in shadow mode — measure proposed-vs-actual accuracy for 30 days before
> enabling execution. Re-evaluate thresholds quarterly against labeled outcomes in vector DB.

---

## 9. Cascading Failure Debounce

**Problem (and why it matters):** When a Dataproc cluster fails to create (R7), every downstream task
in that DAG run — load tasks, sensors, Hudi writes — all fail too. Without debouncing, one cluster failure
generates 15–20 separate events → 15–20 LLM calls → exactly the cost this system was built to avoid.

**Rule: Heal once per root cause, not once per symptom.**

```python
# debouncer/root_cause.py

TASK_DEPENDENCY_ORDER = ['create_cluster', 'wait_touch', 'extract', 'load', 'hudi', 'bq']

def find_root_task(events: List[FailureEvent]) -> FailureEvent:
    """
    Given multiple failures from the same dag_run_id, return the earliest
    task in the dependency chain — that's the root cause. Ignore the rest.
    """
    # Group by dag_run_id
    by_run = defaultdict(list)
    for e in events:
        by_run[e.run_id].append(e)

    root_events = []
    for run_id, run_events in by_run.items():
        # Find task earliest in dependency order
        def task_rank(event):
            for i, pattern in enumerate(TASK_DEPENDENCY_ORDER):
                if pattern in event.task_id.lower():
                    return i
            return 99

        root = min(run_events, key=task_rank)
        root_events.append(root)

    return root_events


# Pub/Sub subscriber: debounce window before triggering AI brain
def debounced_subscriber():
    DEBOUNCE_WINDOW_SECS = 120  # collect all failures for 2 min after first one

    pending: Dict[str, List[FailureEvent]] = defaultdict(list)

    while True:
        event = pubsub.pull()
        pending[event.run_id].append(event)

        # After debounce window, find root cause and heal once
        if time_since_first(pending[event.run_id]) > DEBOUNCE_WINDOW_SECS:
            roots = find_root_task(pending.pop(event.run_id))
            for root in roots:
                trigger_ai_brain(root)   # one call per run, not per task
```

> **Example:** Cluster create fails → 18 downstream tasks fail.
> Without debounce: 18 LLM calls. With debounce: **1 call** on `create_cluster`.
> After root is fixed, the DAG run is cleared — all 18 tasks retry together.

---

## 10. Observability & Cost Tracking

Without metrics, you cannot prove the system works — or justify its cost.

```python
# observability/metrics.py — push to Cloud Monitoring

METRICS = {
    # Effectiveness
    'incidents_total':           Counter,   # all failures received
    'incidents_auto_resolved':   Counter,   # auto-retry succeeded
    'incidents_pr_created':      Counter,   # code PR generated
    'incidents_ops_escalated':   Counter,   # sent to human
    'auto_heal_rate':            Gauge,     # auto_resolved / total (target: 55–65%)

    # Speed
    'mttr_auto_mins':            Histogram, # mean time to resolve (auto path)
    'mttr_pr_merge_mins':        Histogram, # mean time from PR created to merged

    # Quality
    'confidence_score_dist':     Histogram, # distribution of confidence scores
    'pr_merged_without_edit':    Counter,   # dev merged as-is (high AI quality)
    'pr_closed_without_merge':   Counter,   # dev rejected (penalize in vector DB)
    'false_positive_rate':       Gauge,     # PRs that introduced new failures

    # Cost
    'llm_tokens_haiku':          Counter,   # Haiku tokens consumed
    'llm_tokens_opus':           Counter,   # Opus tokens consumed
    'llm_cost_usd':              Counter,   # total $ spent on LLM per day
    'sandbox_cluster_mins':      Counter,   # Tier 2 integration test cluster time
}

# Dashboard: Cloud Monitoring custom dashboard or Grafana
# SLA alert: if auto_heal_rate drops below 40% for 24h → alert eng lead
```

### Bot account blast radius

The PR bot needs write access to create branches and PRs. Limit the damage:

```
Bot account: svc-self-heal-bot (dedicated, not a shared SA)
Permissions:
  - Create branch: YES
  - Push to branch: YES (own branch only, not main/release)
  - Create PR: YES
  - Approve PR: NO  (branch protection rule: bot approvals don't count)
  - Merge PR: NO    (never — developer-only)
  - Delete branch: YES (own branches only, after merge)
  - Access secrets/env vars: NO

Branch naming: self-heal/* (protected: cannot be pushed to by humans accidentally)
```

---

## 11. Cold-Start & Shadow Mode

**Problem:** The vector DB starts empty — no historical incidents to match against.

### Cold-start strategy
1. **Seed with runbooks:** Convert existing Confluence runbooks, past Slack incident threads, and JIRA incident tickets into vector DB entries (without `resolution_successful` set yet)
2. **Shadow mode (Day 1–30):** System classifies and proposes fixes but does NOT execute or create PRs. All proposals are emailed to the team for validation
3. **Supervised mode (Day 30–90):** Auto-retry only (Path A). Code PRs still require human to trigger
4. **Full autonomous mode (Day 90+):** All paths active, PRs auto-created when confidence ≥ 0.90

```python
SYSTEM_MODE = Variable.get('self_heal_mode', default='shadow')
# Values: 'shadow' | 'supervised' | 'full'

def execute(plan, event):
    if SYSTEM_MODE == 'shadow':
        notify_developer(event, plan, mode='advisory')
        record_to_vector_db(event, plan, executed=False)
        return

    if SYSTEM_MODE == 'supervised' and plan.resolution_type != 'auto_retry':
        notify_developer(event, plan, mode='advisory')
        return

    # Full mode: execute based on resolution type
    RESOLUTION_HANDLERS[plan.resolution_type](plan, event)
```

---

## 12. Circuit Breaker

**Problem:** Without a circuit breaker, the AI brain can loop — proposing and retrying the same fix indefinitely on a fundamentally broken pipeline.

```python
# circuit_breaker.py

MAX_AUTO_ATTEMPTS = 3          # per incident type, per 24-hour window
MAX_PR_ATTEMPTS = 1            # one PR per incident — don't spam reviewers
CIRCUIT_OPEN_HOURS = 4         # stop all auto-healing for a DAG after circuit opens

def check_circuit(dag_id: str, bucket: str) -> CircuitState:
    attempts_today = vector_db.count_recent(
        dag_id=dag_id,
        bucket=bucket,
        hours=24,
    )

    if attempts_today >= MAX_AUTO_ATTEMPTS:
        # Open the circuit — all further failures for this DAG go straight to ops
        set_circuit_open(dag_id, duration_hours=CIRCUIT_OPEN_HOURS)
        send_alert(f'Circuit breaker opened for {dag_id}/{bucket} — {attempts_today} attempts today')
        return CircuitState.OPEN

    return CircuitState.CLOSED

# Also: never retry WRITE_APPEND BQ tasks without dedup verification
# (prevents data duplication — see idempotency check below)
```

### Idempotency gate for data tasks

Before any auto-retry of a BQ or Hudi load task:
```python
SAFE_TO_RETRY = {
    'hudi_cow_upsert': True,       # upsert by primary key — safe
    'bq_write_truncate': True,     # overwrites — safe
    'bq_merge': True,              # upsert semantics — safe
    'bq_write_append': False,      # NEVER auto-retry — will duplicate rows
    'gcs_touch_file': True,        # overwrite is harmless
}

if not SAFE_TO_RETRY.get(task_write_mode, False):
    # Force ops escalate — don't risk data duplication
    return escalate_to_ops(plan, event)
```

---

## 13. Feedback Loop

The feedback loop is what converts this system from a static rule engine into a **learning system**. Confidence becomes empirical over time.

```python
# feedback/recorder.py

def record_outcome(event: FailureEvent, plan: ResolutionPlan,
                   resolution_successful: bool, pr_merged: bool = False,
                   time_to_resolve_mins: int = None):
    """
    Called at two points:
    1. After auto-retry: did the pipeline proceed? (webhook from Airflow)
    2. After PR merge: did the code fix solve it? (webhook from GitHub)
    """
    vector_db.upsert(
        id=event.incident_id,
        vector=embed(event.error_message + event.log_snippet),
        metadata={
            **event.to_dict(),
            **plan.to_dict(),
            'resolution_successful': resolution_successful,
            'pr_merged': pr_merged,
            'time_to_resolve_mins': time_to_resolve_mins,
            'outcome_recorded_at': datetime.utcnow().isoformat(),
        }
    )

    # If the proposed fix FAILED after PR merge:
    # flag this pattern so future similarity matches are penalized
    if pr_merged and not resolution_successful:
        vector_db.update_penalty(event.incident_id, penalty_weight=0.3)

    # Trigger XGBoost retraining if > 100 new labeled incidents since last run
    if vector_db.unlabeled_count() > 100:
        trigger_classifier_retrain()
```

### Feedback triggers
- **Auto-retry success:** Airflow task transitions from `up_for_retry` → `success` (Airflow webhook)
- **PR merged as-is:** GitHub webhook `pull_request.merged` + diff unchanged → high quality signal
- **PR merged with edits:** Compare AI-proposed diff vs final merged diff. If edit distance > 30% → penalize
  that pattern. The fix was wrong enough that the developer rewrote it — don't reuse it as a template.
- **PR closed without merge:** penalize that fix pattern in vector DB
- **Manual fix required after auto-attempt:** developer records via Slack command `/self-heal feedback {incident_id} failed`

```python
# GitHub webhook handler
def on_pr_merged(pr_event):
    incident_id = extract_incident_id(pr_event['pull_request']['body'])
    ai_diff = vector_db.get(incident_id)['fix_diff']
    final_diff = github.get_merged_diff(pr_event['pull_request']['number'])

    edit_distance_pct = diff_similarity(ai_diff, final_diff)

    record_outcome(
        incident_id=incident_id,
        resolution_successful=True,
        pr_merged=True,
        ai_fix_accuracy=1.0 - edit_distance_pct,  # 1.0 = merged as-is, 0.3 = heavily edited
    )
```

---

## 14. Implementation Roadmap

| Phase | What | Deliverable | Duration |
|-------|------|-------------|----------|
| **Phase 0** | Layers 1–4 (deterministic, in-DAG) | Safe operators, retry-with-cleanup, watchdog | 1 week |
| **Phase 1** | Failure taxonomy + log labeling | 200+ labeled incidents in spreadsheet → seed vector DB | 2 weeks |
| **Phase 2** | XGBoost classifier | Trained model, Cloud Run serving, shadow-mode alerts | 3 weeks |
| **Phase 3** | Vector DB + Orchestrator | Pinecone/Vertex setup, similarity search, shadow proposals | 3 weeks |
| **Phase 4** | Resolution executor (Path A only) | Auto-retry for R1/R5/R7 in supervised mode | 2 weeks |
| **Phase 5** | Sandbox + PR factory | Cloud Build test env, GitHub PR creation, Snyk gate | 4 weeks |
| **Phase 6** | Full autonomous mode | Confidence-gated execution, feedback loop, circuit breaker | 2 weeks |
| **Ongoing** | Classifier retraining | Weekly model refresh from new incidents | Automated |

**Total to MVP (Phase 0–4): ~11 weeks**
**Full system: ~17 weeks**

---

## Failure Bucket → Resolution Path Quick Reference

```
R1 Resource (OOM/quota)     → Layer 1-2 auto-retry → if fails: scale config PR
R2 Dependency mismatch      → Code PR (Opus for analysis) → sandbox → Snyk → PR
R3 Auth / token expired     → OPS ESCALATE ONLY (no auto-fix, token rotation needed)
R4 Cert expired             → OPS ESCALATE ONLY (no auto-fix, cert renewal needed)
R5 Network / timeout        → Layer 2 auto-retry with backoff → if persistent: ops
R6 Data / schema failure    → Code/config PR (analyze schema diff) → sandbox → PR
R7 Cluster failure          → Layer 1 idempotent ops → auto-retry → if ERROR: ops
R8 DAG / code failure       → Code PR (Opus) → sandbox tests → Snyk → PR
R9 External dependency      → Wait-retry → if upstream stuck: watchdog clears zombie
```

---

## Repo Coverage

| Repo | Layers covered | Notes |
|------|---------------|-------|
| `Finance-eComm/FDL_CoreFinance` | All layers | Hudi CoW (idempotent), GCS `.done` touch files, YAML DAGs |
| `SamsDSE/finance-neptune-airflow` | All layers | Delta Lake on Databricks (idempotent), Python DAGs, ExternalTaskSensor |

---

*Designed for Walmart Finance Data Engineering — GCP Composer (Airflow 2.x) + Dataproc + Databricks*
*AI components: Claude Opus 4 (novel/complex), Claude Haiku 4 (high-volume/known), XGBoost (classifier)*
