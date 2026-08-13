# Self-Healing Airflow + Dataproc Pipeline Architecture

> Covers: Finance-eComm (`FDL_CoreFinance`) and Sam's Club (`finance-neptune-airflow`) Airflow pipelines on GCP.

---

## Problem Statement

| Symptom | Root Cause |
|---------|-----------|
| Delete Cluster task fails | Fixed cluster name — if already deleted, operator gets `404 NOT_FOUND` and throws |
| Auto-restart fails repeatedly | Retry replays the same broken state (orphan cluster, stale touch file) |
| Only manual restart works | Human clears the broken state before re-triggering |
| `max_active_runs: 1` blocks new runs | Zombie/stuck run never transitions to `failed` — blocks all future scheduled runs |

**Core principle:** Auto-restart fails because retries hit the same broken state. Self-healing = **detect broken state → remediate it → then retry**.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│  Layer 4: Escalation                                     │
│  Cloud Monitoring → Pub/Sub → Cloud Function → Email     │
│  (fires only after all retries exhausted)                │
├─────────────────────────────────────────────────────────┤
│  Layer 3: External Watchdog  (Cloud Function / Run)      │
│  Cloud Scheduler (every 15 min) → Airflow REST API       │
│  Detects zombie runs (running > 6h) → marks failed       │
│  Unblocks max_active_runs=1 for next scheduled run       │
├─────────────────────────────────────────────────────────┤
│  Layer 2: Retry-with-Cleanup  (in-DAG)                   │
│  on_retry_callback tears down broken state before retry  │
│  • Deletes orphan cluster before recreating              │
│  • Logs touch file status on sensor timeout              │
│  retries=3, exponential backoff, max_retry_delay=30m     │
├─────────────────────────────────────────────────────────┤
│  Layer 1: Idempotent Operators  (in-DAG)                 │
│  Each task handles "already done" / "not found" cleanly  │
│  • safe_delete_cluster: 404 → success (skip)             │
│  • safe_create_cluster: AlreadyExists+RUNNING → reuse    │
│  • trigger_rule=all_done on delete → always runs         │
└─────────────────────────────────────────────────────────┘
```

---

## Layer 1: Idempotent Operators

### `plugins/safe_dataproc_ops.py`

```python
from google.cloud import dataproc_v1
from google.api_core import exceptions as gcp_exceptions
import logging


def safe_delete_cluster(project_id, region, cluster_name, **context):
    """Delete Dataproc cluster; treat NotFound as success (idempotent)."""
    client = dataproc_v1.ClusterControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )
    try:
        op = client.delete_cluster(
            request={"project_id": project_id, "region": region, "cluster_name": cluster_name}
        )
        op.result(timeout=300)
        logging.info(f"Cluster {cluster_name} deleted successfully.")
    except gcp_exceptions.NotFound:
        logging.info(f"Cluster {cluster_name} already gone — skipping delete. ✓")
    except gcp_exceptions.FailedPrecondition as e:
        logging.warning(f"Cluster {cluster_name} already being deleted: {e}")


def safe_create_cluster(project_id, region, cluster_name, cluster_config, **context):
    """Create Dataproc cluster; if RUNNING already exists, reuse it."""
    client = dataproc_v1.ClusterControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )
    try:
        op = client.create_cluster(
            request={"project_id": project_id, "region": region, "cluster": cluster_config}
        )
        op.result(timeout=600)
        logging.info(f"Cluster {cluster_name} created.")
    except gcp_exceptions.AlreadyExists:
        cluster = client.get_cluster(
            project_id=project_id, region=region, cluster_name=cluster_name
        )
        state = cluster.status.state.name
        if state == "RUNNING":
            logging.info(f"Cluster {cluster_name} already RUNNING — reusing. ✓")
        else:
            raise RuntimeError(
                f"Cluster {cluster_name} exists but in {state} state. "
                f"Delete it manually or wait for cleanup."
            )
```

### Wire into DAG

```python
from airflow.operators.python_operator import PythonOperator
from safe_dataproc_ops import safe_delete_cluster, safe_create_cluster

# Create cluster (T1A equivalent)
create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=safe_create_cluster,
    op_kwargs={
        'project_id': PROJECT_ID,
        'region': REGION,
        'cluster_name': CLUSTER_NAME,
        'cluster_config': CLUSTER_CONFIG,
    },
)

# Delete cluster — ALWAYS runs, even if upstream fails
delete_cluster = PythonOperator(
    task_id='delete_cluster',
    python_callable=safe_delete_cluster,
    op_kwargs={
        'project_id': PROJECT_ID,
        'region': REGION,
        'cluster_name': CLUSTER_NAME,
    },
    trigger_rule='all_done',   # ← KEY: runs even if upstream tasks fail
)
```

---

## Layer 2: Retry-with-Cleanup

### `plugins/healing_callbacks.py`

```python
from google.cloud import storage, dataproc_v1
from google.api_core import exceptions as gcp_exceptions
import logging


def cleanup_before_retry(context):
    """
    on_retry_callback: called by Airflow automatically before each retry.
    Tears down broken state so the retry starts clean.
    """
    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id
    exception = context.get('exception')

    logging.warning(f"[SELF-HEAL] Retry triggered — {dag_id}.{task_id}: {exception}")

    if 'create_cluster' in task_id:
        _force_delete_orphan_cluster(context)

    if any(kw in task_id.lower() for kw in ['wait_touch', 'sensor', 't1b', 't1c', 't1d']):
        _log_touch_file_status(context)

    # Clear stale XCom from failed attempt
    context['task_instance'].clear_xcom_data()


def _force_delete_orphan_cluster(context):
    params = context.get('params', {})
    cluster_name = params.get('cluster_name')
    project_id = params.get('project_id')
    region = params.get('region', 'us-central1')

    if not all([cluster_name, project_id]):
        return

    client = dataproc_v1.ClusterControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )
    try:
        op = client.delete_cluster(
            request={"project_id": project_id, "region": region, "cluster_name": cluster_name}
        )
        op.result(timeout=120)
        logging.info(f"[SELF-HEAL] Orphan cluster {cluster_name} deleted before retry.")
    except gcp_exceptions.NotFound:
        pass  # Already gone — fine


def _log_touch_file_status(context):
    """Log touch file presence to distinguish sensor timeout vs upstream never wrote."""
    params = context.get('params', {})
    bucket = params.get('touch_file_bucket')
    prefix = params.get('touch_file_prefix')

    if bucket and prefix:
        client = storage.Client()
        blobs = list(client.list_blobs(bucket, prefix=prefix, max_results=10))
        logging.info(
            f"[SELF-HEAL] Touch files at gs://{bucket}/{prefix}: "
            f"{[b.name for b in blobs] or 'NONE FOUND'}"
        )


def escalate_to_oncall(context):
    """
    on_failure_callback: fires only after all retries exhausted.
    Classifies the failure and sends a targeted alert.
    """
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    exception = context.get('exception', 'Unknown')
    run_id = context.get('run_id', '')

    err = str(exception).lower()

    if 'permission' in err or 'forbidden' in err or '403' in err:
        severity, action = 'HIGH', 'Check service account IAM roles in GCP console'
    elif 'quota' in err or 'resource_exhausted' in err:
        severity, action = 'MEDIUM', 'Dataproc quota hit — stagger DAG schedules or request quota increase'
    elif 'not found' in err or '404' in err:
        severity, action = 'LOW', 'Resource not found — idempotent fix should handle; check Dataproc operations log'
    elif 'timeout' in err:
        severity, action = 'MEDIUM', 'Operation timed out — check GCP Dataproc operations for stuck DELETING/CREATING state'
    else:
        severity, action = 'HIGH', 'Unknown error — check Airflow task logs immediately'

    from airflow.utils.email import send_email
    send_email(
        to=["GECGISFI28@email.wal-mart.com"],
        subject=f"[{severity}] Self-heal exhausted: {dag_id}.{task_id}",
        html_content=f"""
        <h3>Pipeline Self-Heal Exhausted</h3>
        <table>
          <tr><td><b>DAG</b></td><td>{dag_id}</td></tr>
          <tr><td><b>Task</b></td><td>{task_id}</td></tr>
          <tr><td><b>Run ID</b></td><td>{run_id}</td></tr>
          <tr><td><b>Error</b></td><td>{exception}</td></tr>
          <tr><td><b>Severity</b></td><td>{severity}</td></tr>
          <tr><td><b>Action</b></td><td>{action}</td></tr>
        </table>
        """
    )
```

### Wire into DAG `default_args`

```python
from healing_callbacks import cleanup_before_retry, escalate_to_oncall
from datetime import timedelta

default_args = {
    # Was: retries: 0  →  now:
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'on_retry_callback': cleanup_before_retry,    # ← cleans broken state
    'on_failure_callback': escalate_to_oncall,    # ← fires after all retries fail
}
```

---

## Layer 3: External Watchdog

Deploys as a **Cloud Function** triggered every 15 minutes by Cloud Scheduler.
Targets the `max_active_runs: 1` zombie-run problem.

### `watchdog/main.py`

```python
import requests
from datetime import datetime, timezone
import logging

AIRFLOW_API_BASE = "https://<your-composer-webserver-url>/api/v1"
AIRFLOW_TOKEN = "<service-account-token>"   # Use Workload Identity in prod

HEADERS = {
    "Authorization": f"Bearer {AIRFLOW_TOKEN}",
    "Content-Type": "application/json",
}

# DAGs to monitor — add all critical pipeline DAGs
MONITORED_DAGS = [
    "US-WM-FINECOMM-CORE-GCS-INC-LOAD-HLY-WM_SALES_ORDER_INV_CHRG_DTL",
    "US-WM-FINECOMM-CORE-GCS-INC-LOAD-HLY-WM_SALES_ORDER_INV_TNDR_DTL",
    # finance-neptune-airflow DAGs:
    "pbc_sales_p1_metrics",
    "pbc_inventory_p1_metrics",
    "pbc_profit_p1_metrics",
]

MAX_RUNTIME_HOURS = 6  # DAGs running longer than this are considered zombies


def watchdog(request):
    """Entry point for Cloud Function."""
    zombies_cleared = []

    for dag_id in MONITORED_DAGS:
        try:
            runs = _get_running_runs(dag_id)
            for run in runs:
                age_hours = _age_hours(run["start_date"])
                if age_hours > MAX_RUNTIME_HOURS:
                    logging.warning(
                        f"[WATCHDOG] Zombie: {dag_id}/{run['dag_run_id']} "
                        f"({age_hours:.1f}h old)"
                    )
                    _clear_zombie_run(dag_id, run["dag_run_id"])
                    zombies_cleared.append(f"{dag_id}/{run['dag_run_id']}")
        except Exception as e:
            logging.error(f"[WATCHDOG] Error checking {dag_id}: {e}")

    return {"cleared": zombies_cleared, "checked": len(MONITORED_DAGS)}


def _get_running_runs(dag_id):
    resp = requests.get(
        f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns",
        params={"state": "running", "limit": 10},
        headers=HEADERS,
    )
    resp.raise_for_status()
    return resp.json().get("dag_runs", [])


def _age_hours(start_date_str):
    start = datetime.fromisoformat(start_date_str.replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    return (now - start).total_seconds() / 3600


def _clear_zombie_run(dag_id, dag_run_id):
    # 1. Mark all running/queued tasks as failed
    tasks_resp = requests.get(
        f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
        params={"state": ["running", "queued"]},
        headers=HEADERS,
    )
    for task in tasks_resp.json().get("task_instances", []):
        requests.patch(
            f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns/{dag_run_id}"
            f"/taskInstances/{task['task_id']}",
            json={"new_state": "failed"},
            headers=HEADERS,
        )

    # 2. Mark the DAG run itself as failed
    requests.patch(
        f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns/{dag_run_id}",
        json={"state": "failed"},
        headers=HEADERS,
    )
    logging.info(f"[WATCHDOG] Cleared zombie: {dag_id}/{dag_run_id}")
```

### `watchdog/deploy.sh`

```bash
gcloud functions deploy airflow-watchdog \
  --runtime python311 \
  --trigger-http \
  --entry-point watchdog \
  --region us-central1 \
  --service-account <airflow-sa>@<project>.iam.gserviceaccount.com \
  --set-env-vars AIRFLOW_API_BASE=https://<composer-url>/api/v1

# Cloud Scheduler job — every 15 minutes
gcloud scheduler jobs create http airflow-watchdog-trigger \
  --schedule "*/15 * * * *" \
  --uri https://us-central1-<project>.cloudfunctions.net/airflow-watchdog \
  --http-method POST \
  --location us-central1
```

---

## Layer 4: Failure Classification & Escalation

The `escalate_to_oncall` callback (in Layer 2) classifies failures before alerting:

| Error signal | Severity | Auto-retry? | Human action |
|---|---|---|---|
| `PERMISSION_DENIED` / 403 | HIGH | ✗ No | Check SA IAM roles |
| `RESOURCE_EXHAUSTED` / quota | MEDIUM | ✗ No | Request quota increase / stagger schedules |
| `NOT_FOUND` / 404 | LOW | ✓ Yes (idempotent) | Check Dataproc ops log if persistent |
| Timeout / `DEADLINE_EXCEEDED` | MEDIUM | ✓ Yes | Check GCP ops for stuck state |
| Unknown | HIGH | ✓ Yes (limited) | Check Airflow logs |

---

## Implementation Roadmap

| Priority | Change | Fixes | Effort |
|---|---|---|---|
| **P0 — Do today** | `trigger_rule='all_done'` on all delete cluster tasks | Cluster blocks next run | 5 min/DAG |
| **P0 — Do today** | `safe_delete_cluster` (handles 404 as success) | Delete Cluster failure | 1 hour |
| **P1 — This week** | `on_retry_callback=cleanup_before_retry` + `retries=3` | Auto-restart failure | 2 hours |
| **P2 — Next sprint** | External Watchdog Cloud Function | Zombie stuck runs | 1 day |
| **P2 — Next sprint** | `safe_create_cluster` (handles AlreadyExists) | Race on re-runs | 2 hours |
| **P3 — Later** | Full escalation with failure classification email | Better on-call routing | 3 hours |

---

## Prerequisite: Idempotency Check

Before increasing `retries`, verify each task is safe to re-run:

| Task type | Re-run safe? | Notes |
|---|---|---|
| Hudi CoW upsert | ✓ Yes | Upserts by primary key — re-running won't duplicate |
| BQ `WRITE_TRUNCATE` | ✓ Yes | Overwrites — safe |
| BQ `MERGE` | ✓ Yes | Upsert semantics |
| BQ `WRITE_APPEND` | ✗ No | Will duplicate rows — add dedup or switch to MERGE first |
| GCS touch file create | ✓ Yes | Overwriting same `.done` file is harmless |
| Dataproc cluster create | ✓ Yes (after fix) | `safe_create_cluster` handles AlreadyExists |

---

## Repos This Applies To

| Repo | Platform | Notes |
|---|---|---|
| `Finance-eComm/FDL_CoreFinance` | YAML DAG configs (Aeolus) | Hudi CoW on GCS, `.done` touch files, GCS sensors |
| `SamsDSE/finance-neptune-airflow` | Python DAG files | Delta Lake on Databricks, `ExternalTaskSensor` — Layer 1 & 2 port directly; Layer 3 watchdog DAG list differs |

---

*Designed for Walmart Finance Data Engineering pipelines — GCP Composer (Airflow 2.x) + Dataproc*
