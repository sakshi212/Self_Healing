# CI/CD Maturity, AutoCRQ & ASD — Engineering Reference
### EBS Engineering Excellence · Walmart Finance Tech / AEDT
### Sources: SST Confluence — CICD Maturity Levels · AutoCRQ Process with ASD · CICD Pain Points (all sub-pages)

---

## Table of Contents

1. [What is ASD and Why It Matters](#1-what-is-asd-and-why-it-matters)
2. [CI/CD Maturity Model — 6 Levels](#2-cicd-maturity-model--6-levels)
3. [AutoCRQ Process — End to End](#3-autocrq-process--end-to-end)
4. [ASD Badge → AutoCRQ Sequencing](#4-asd-badge--autocrq-sequencing)
5. [Allowed vs Excluded Change Types](#5-allowed-vs-excluded-change-types)
6. [CI/CD Pain Points — All Pillars](#6-cicd-pain-points--all-pillars)
7. [AEDT / Data Pipeline Specific Blockers](#7-aedt--data-pipeline-specific-blockers)
8. [Rollback Strategy — Canary vs Rollr](#8-rollback-strategy--canary-vs-rollr)
9. [Infiniti 2.0 — Adoption Status & Open Issues](#9-infiniti-20--adoption-status--open-issues)
10. [AI-Assisted Monitoring & MCP Guidance](#10-ai-assisted-monitoring--mcp-guidance)
11. [DORA Metrics Targets](#11-dora-metrics-targets)
12. [Consolidated Action Items & Owners](#12-consolidated-action-items--owners)
13. [Key Toolchain Reference](#13-key-toolchain-reference)

---

## 1. What is ASD and Why It Matters

**ASD (Automated Safe Deployment)** is Walmart EBS's safety and quality framework that defines a pipeline's readiness before any automated deployment is allowed.

**AutoCRQ (Automated Change Request)** grants two privileges once ASD criteria are met:
1. CRQs are auto-generated and auto-approved by the pipeline
2. Changes proceed to production without manual human intervention

> *"You can't automate deployments without having confidence that they are safe."*

The critical sequencing: **ASD badge first, AutoCRQ second.** Teams that attempt to get AutoCRQ approval without an ASD badge go through a significantly longer, full manual review path.

**FY27 program goal:** 100% of homegrown APMs achieve ASD Level 3 (metrics-based) deployment compliance.

---

## 2. CI/CD Maturity Model — 6 Levels

### Level 1 — Foundational CI

| Requirement | Detail |
|-------------|--------|
| GitHub hosting | Repo hosted in GitHub, tagged to correct Team Roster product team, associated with APM ID |
| Branch protection | Main branch protected — minimum **2 approvers** required to merge any PR |
| SonarQube onboarded | Repo integrated with SonarQube for static analysis |
| Build & test automation | Looper + Concord for: build steps, unit & integration tests, CodeGate scans, Artifactory artifact upload |

**Key outcome:** Foundational CI established. Code scanned. Basic branch protection enforced.

---

### Level 2 — Quality Gates & Monitoring

| Requirement | Detail |
|-------------|--------|
| SonarQube PR decoration | Inline SonarQube feedback on every pull request |
| "Sonar Way" compliance | New code meets default Sonar quality gate: **≥80% code coverage**, Grade A for maintainability, reliability, security |
| Critical integration tests | Run in QA environment covering essential user flows, approved by product team |
| Monitoring & alerting | Basic monitoring via Golden Signal, MMS, and Spotlight |

**Key outcome:** Stricter quality standards. Sonar gates. Initial integration testing. Production-like monitoring.

---

### Level 3 — Zero High-Severity Issues & Automated Deployment

| Requirement | Detail |
|-------------|--------|
| Zero SonarQube blockers | Zero "Blocker" findings (Bugs, Vulnerabilities, Code Smells) across **entire codebase** |
| Sonar Way across all code | Not just new code — full codebase compliance with Sonar Way quality gate |
| QA E2E gate | End-to-end testing gates enforced after every QA deployment before environment promotion |
| No manual production changes | Except for Change Control Management (CCM), all production changes go through automated pipeline |

**Key outcome:** Zero high-severity issues. Automated deployment flow. Minimal manual intervention.

> **AutoCRQ eligibility starts at Level 3 (Gold badge).** This is the ASD certification threshold.

---

### Level 4 — Performance Testing & DORA Metrics

| Requirement | Detail |
|-------------|--------|
| Performance testing | Mandatory for high-transaction applications |
| Synthetic load testing | Required for other systems in Stage/Pre-Prod |
| Automatic smoke tests | Triggered as part of every production deployment pipeline |
| Auto CRQ enabled | Automated CRQ approvals for eligible operational change types |
| DORA metrics tracking | Deployment frequency, lead time, change failure rate, MTTR measured and actioned |

**Key outcome:** Resiliency through performance testing. Automated smoke checks. Data-driven continuous improvement via DORA.

---

### Level 5 — Shift-Left, Zero-Click Deployment & Strong DORA

| Requirement | Detail |
|-------------|--------|
| API linting | Gate enforced in **Development** environment |
| Contract testing | Service interactions validated in Development via contract testing |
| Mock/localized integration tests | Run against mocks earlier in the lifecycle — detect issues before QA |
| Zero-click deployment | After successful test gates, changes flow automatically to next environment — no manual promotion |
| All stage gates automated | Unit → Integration → E2E → Contract → API Linting → Performance → Smoke tests, all gated |

**DORA targets at Level 5:**

| Metric | Target |
|--------|--------|
| Development cycle time | **< 24 hours** |
| Deployment frequency | **≥ 5 per week** |
| Change failure rate | **≤ 15%** |
| MTTR | **≤ 60 minutes** |

**Key outcome:** Deep shift-left testing. Near-seamless automated deployment. Strong DORA metrics.

---

### Level 6 — Enterprise-Grade Quality & Safety

| Requirement | Detail |
|-------------|--------|
| Advanced Sonar quality profiles | Stricter SonarQube thresholds beyond default "Sonar Way" |
| Automated production rollback | Pipeline includes automated rollback — reduces downtime on problematic releases |
| High availability | Production runs with **≥ 2 pods** for failover and resilience |
| Autoscaling | Stage and Production automatically scale resources based on load |
| Trunk-based development | All feature branches from trunk, **deleted within 24 hours** |

**Key outcome:** Enterprise-grade quality. Automated rollback. Robust HA/DR. Streamlined short-lived branching.

---

### Maturity Level Quick Reference

| Level | Name | AutoCRQ | Key Gate |
|-------|------|---------|---------|
| 1 | Foundational CI | ❌ | GitHub + SonarQube onboarding |
| 2 | Quality Gates | ❌ | ≥80% coverage, Sonar Way |
| 3 | Automated Deployment | ✅ **Eligible** | Zero blockers, E2E gate, no manual prod changes |
| 4 | Performance + DORA | ✅ **Enabled** | Smoke tests, AutoCRQ, performance testing |
| 5 | Shift-Left + Zero-Click | ✅ | API linting, contract tests, DORA targets met |
| 6 | Enterprise Safety | ✅ | Auto rollback, HA, trunk-based dev |

---

## 3. AutoCRQ Process — End to End

### Path A: ASD Gold Badge (Level 3) — Fast-Track

```
Step 1: Achieve ASD Gold Badge
  └── Submit EBSASD Jira ticket for badge review
  └── Reviewed by: APM SMEs + Architects + ASD Program reps
  └── Badge awarded → error budget assigned to APM

Step 2: Submit RITM Request
  └── Include: ASD Gold badge (EBSASD Jira ticket #)
  └── Include: Standard AutoCRQ evidence package
  └── Absence of ASD badge = full standard review (much slower)

Step 3: Fast-track to VP + CAB Approvers
  └── Badge review pre-satisfies all pipeline readiness gates
  └── Approvers review only change-specific details
  └── No re-litigation of pipeline readiness

Step 4: Auto-CRQ Gate in CI/CD Pipeline
  └── Branch must be named: /autocrq_<JIRA#>  ← mandatory convention
  └── LLM scans PR → validates only low-risk operational intent
  └── Confirms JIRA ticket status = "Done"
  └── All checks pass → CRQ auto-generated + auto-approved

Step 5: Auto-Deploy to Production
  └── Last known good rollback build captured (Rollr watermark)
  └── Real-time observability via TeamProductivity dashboards
  └── 1-click rollback readiness confirmed via Rollr
```

### Path B: Non-ASD APMs — Standard Path

For vendor-managed pipelines, mainframe tooling, or apps not eligible for ASD badge:
- Full standard AutoCRQ review sequence applies
- ASD badge criteria do not apply
- Review by ASD guardians / CICD Architects → VP → CAB

---

## 4. ASD Badge → AutoCRQ Sequencing

```
ASD Badge Levels:

Level 1 (Bronze)  →  Basic CI established
Level 2 (Silver)  →  Quality gates passing
Level 3 (Gold)    →  AutoCRQ ELIGIBLE ← submit RITM here
Level 4 (Gold+)   →  AutoCRQ enabled + performance testing
Level 5 (Platinum)→  Zero-click deployment
Level 6 (Diamond) →  Enterprise-grade safety

AutoCRQ flow:

RITM submitted
    │
    ├─ ASD Gold badge present?
    │      YES → Fast-track to VP + CAB (days)
    │      NO  → Full review: CICD Architects → VP → CAB (weeks)
    │
    ▼
CI/CD Gate executes on push:
    Branch = /autocrq_<JIRA#>?   ✓
    LLM PR scan = low-risk ops?   ✓
    JIRA status = Done?           ✓
    ↓
    CRQ auto-generated + auto-approved
    ↓
    Production deploy
    ↓
    Rollback watermark captured
```

**Approval cadence and performance targets:**

| Metric | Target |
|--------|--------|
| AutoCRQ success rate | > 95% |
| Rollback incidence | < 1% |
| MTTR (P0/P1) | < 30 minutes |
| Deployment frequency | ≥ 5 per week |
| Dev cycle time (commit → prod) | < 24 hours |
| Change failure rate | < 15% |
| Governance audits | Monthly |
| Error budget review per APM | Monthly |

---

## 5. Allowed vs Excluded Change Types

| ✅ Allowed via AutoCRQ | ❌ Requires Full ASD Review |
|-----------------------|---------------------------|
| Credential rotations | Code fixes and bug patches |
| Certificate renewals | Library / framework upgrades |
| Vault key updates | Feature enhancements |
| Runtime config changes | Schema / infrastructure changes |

> **Important:** AutoCRQ is for **routine operational changes only** — not code. Any PR that touches application logic goes through full ASD.

---

## 6. CI/CD Pain Points — All Pillars

### Impact severity framework

| Level | Risk to |
|-------|---------|
| 🔴 High | Safe deployment |
| 🟡 Medium | 100% CI/CD automation |
| 🟢 Low | Error budget metrics emission |

---

### PeopleTech Blockers

Two gaps blocking CI/CD commitment:
1. Automatic rollback — no clear implementation path defined yet
2. Error margin tracking — metrics graph definition in progress

---

### FinTech — 8 Pain Points

**🔴 P1: ITSM & CAB approval lead times** *(High — blocks safe deployment)*

ITSM SLA = 5 days, but regularly overshoots by **10–15 days**. CAB review only on alternate days. Result: planned timelines slip by **more than a month**.

*Workaround:* Submit to Architect Champions before ITSM/CAB to reduce back-and-forth. Plan timelines assuming a full month of approval cycle.

**🟡 P2: DevOps score only measured for WCNP** *(Medium)*

No solution for measuring DevOps maturity for UDP deployments or non-WCNP deployment types. To be discussed with GTP.

**🟡 P3: Metrics not applicable to batch apps** *(Medium)*

API Lifecycle Manager, API Linter, and Canary analysis are mandated in the maturity model but not all are relevant for batch applications. Needs GTP clarification on which requirements apply to batch vs API workloads.

**🟡 P4: TestHub/TestBurst not integrated for Scala Sbt builds** *(Medium)*

Tracking with GTP.

**🔴 P5: No APIs for DX portal resource creation** *(High — blocks 100% CI/CD)*

10 resources require manual creation per environment (dev/QA/stage/prod) with no API or Terraform support:

| # | Resource needing API |
|---|---------------------|
| 1 | Kafka cluster creation |
| 2 | Kafka topic creation |
| 3 | WCNP namespace provisioning |
| 4 | Cloud SQL (MySQL/PostgreSQL MSO) provisioning |
| 5 | Hawkshaw onboarding |
| 6 | SSaaS namespace provisioning |
| 7 | Venafi certificate updates |
| 8 | UDP onboarding |
| 9 | AFaaS onboarding |
| 10 | DPaaS onboarding |

**Consequences:** Repeated manual work per environment, human errors, no version tracking, no Terraform support, significant time waste across all teams.

**Preferred solution:** Terraform-like tooling for all infra resources. GTP roadmap pending (Q3 FY26).

---

### ADE — 4 Pain Points

| Pain Point | Impact | Status |
|------------|--------|--------|
| Limited CI/CD tooling for Non-WCNP apps | Commitment assessment blocked | Escalated to GTP |
| SonarQube doesn't support Terraform and PowerShell static analysis | Level 1 completion blocked | Escalated to GTP |
| PowerShell not supported in CodeGate scans | Level 1 completion blocked | Escalated to GTP |
| TestHub connectors not available for Non-WCNP | Custom CI/CD pipeline gap | Escalated to GTP |
| No Concord task to get metrics from MMS + trigger rollback | Custom pipeline rollback impossible | Team connection needed |

---

## 7. AEDT / Data Pipeline Specific Blockers

These three blockers directly affect data engineering pipelines (Airflow, Spark, GCP/Astronomer):

### Blocker 1 — Data Pipeline Monitoring & Alerting at cloud/3rd-party level only 🔴

GCP/Astronomer monitoring currently operates at the cloud and third-party level only. There is no customized way to trigger automated rollback from pipeline-specific metrics (e.g., Airflow task failure rate, Dataproc job SLA breach, data freshness lag).

**Impact:** Level 4 completion blocked — automated rollback using metrics is a Level 4 requirement, but data pipelines can't emit or act on those metrics through the standard Rollr/Canary mechanism.

**Path forward:** Data pipeline monitoring needs a custom bridge from Airflow/Astronomer metrics → Prometheus/MMS → Rollr trigger. This is not yet a supported pattern — WCNP Rollr team engagement required.

---

### Blocker 2 — WCNP Rolling Updates don't support discrete 1-click progression 🟡

WCNP rolling updates are fully automated — there is no discrete 1-click progression gate available. Level 3 ASD requires a discreet 1-click progression step as part of the deployment safety model.

**Impact:** Level 3 compliance issue for services deployed via WCNP rolling updates.

---

### Blocker 3 — WCNP Rollr slow bug fix support 🔴

JIRA: PGPTOOLS-331318. Rollr bugs are unresolved, forcing teams to rely on manual monitoring and manual rollback even when they should be at Level 4 auto-rollback.

**Impact:** Level 4 completion blocked. Teams are stuck in manual remediation mode despite meeting the other Level 4 criteria.

---

### Data Pipeline E2E post-deploy hook rollback gap 🔴

Looper-based E2E tests configured as post-deploy hooks do **not** trigger rollback on failure. If post-deploy E2E tests fail in production:
- A **Change Failure** is recorded in DORA metrics
- **No rollback is triggered**
- The pipeline remains in a failed/degraded state until manual intervention

This is a direct production risk for data pipelines — a failed post-deploy E2E test should trigger automatic rollback, but the toolchain does not support this today.

---

### Gatekeeper validation sequencing gap 🔴

Currently, Gatekeeper validates gates (site status, Snyk, APM) **after** the CRQ is created in production. If a gate fails post-CRQ creation, the CRQ is closed as unsuccessful — wasting the approval cycle.

**Preferred fix:** Gate validation should happen **before** CRQ creation. This is a known platform issue — Infiniti 2.0 fixes this (see §9).

---

## 8. Rollback Strategy — Canary vs Rollr

Choose the right rollback mechanism based on workload type:

| Factor | Canary | Rollr |
|--------|--------|-------|
| **Non-HTTP traffic** (Kafka, ETL, batch) | ❌ Cannot perform canary analysis | ✅ Use Rollr |
| **Business metrics monitoring** | ⚠️ Runs during deployment (often off-peak) | ✅ Monitors beyond deployment window |
| **Non-business metrics** (error rates, latency) | ✅ Supported | ✅ Supported |
| **Automated rollback** | ✅ Full auto-rollback during canary analysis | ❌ 1-click only today (auto coming in beta) |
| **1-click rollback via Slack** | ✅ Default webhooks | ✅ Supported |
| **Cost** | 💰 Charges against namespace quota | Free |

**For data pipelines (Kafka, ETL, Airflow, Spark):** Canary cannot analyze non-HTTP traffic. **Rollr is the correct choice.** However, automated Rollr is not yet GA — see EBS Rollr Initiative below.

### EBS Rollr Initiative (Beta — Aug–Sep 2026)

**Goal:** 15% of EBS pipelines at Level 4 (rollback triggered automatically by metrics)

| Feature | JIRA | Target delivery |
|---------|------|----------------|
| Automated rollback based on metrics | STRCONTAIN-36374 | Sep 1, 2026 |
| Watermark version for rollback | — | Aug 15, 2026 |

**Beta requirements for onboarded teams:**
- Define and maintain a **watermark version** (last known stable) — must be within last 7 Helm history versions
- App team owns the metrics that trigger rollback — including false positive risk from upstream/downstream issues
- Set thresholds carefully — platform/network errors can breach metrics and trigger unintended rollback

**Rollback is NOT supported for (even in beta):**

| Scenario | Why not supported |
|----------|-----------------|
| Apps using Canary deployments | Conflict between two rollback mechanisms |
| Deployments with post-deploy hooks that modify behavior | Hook state can't be undone |
| Multi-stage KITT deployments | Rollback only hits the last stage |
| Networking changes (probes, CNAMEs, GSLBs) | Network state rollback not supported |
| DB schema changes without backward compatibility | Data rollback not possible |
| Secret/password rotations | Cannot un-rotate |

> **Critical constraint:** Rollback only works in **main/base namespaces** — NOT temp namespaces. Temp namespaces don't maintain Helm deployment history.

**Teams currently piloting Rollr beta:** FinTech, PeopleTech, Tx System, CATech, AEDT (InStore Retail Media, Lookup Tools, Campus Hub), Care Org.

---

## 9. Infiniti 2.0 — Adoption Status & Open Issues

Infiniti 2.0 addresses several gaps that exist in the standard Kitt + Looper + Concord approach:

### Infiniti advantages over Kitt

| Capability | Kitt | Infiniti 2.0 |
|-----------|------|-------------|
| Rollback on test failures | ❌ | ✅ |
| Gatekeeper validation **before** prod deployment | ❌ | ✅ |
| CCM updates with AutoCRQ | ❌ | ✅ |
| One bundle release | ❌ | ✅ |
| Automated PRs via Infiniti Engine | ❌ | ✅ |
| Pre-defined pipeline templates | ❌ | ✅ |

### Open issues (as of Aug 2026)

| Issue | Category | Impact | Status |
|-------|----------|--------|--------|
| Manual tweaks needed for Node repos with Custom Docker | Onboarding | High | 🔴 Open |
| No component to monitor metrics from source + trigger progression/rollback | Limitation | Low | 🔴 Open |
| Monorepo support | Onboarding | Tech strategy needed | 🔴 Open |
| Rollr + Infiniti integration | Integration | — | 🔴 Open |

### Current onboarding status

| Team | Repo | Status |
|------|------|--------|
| FinTech | dgc-service (DPA) | 🔄 In Progress — secret passing failing |
| RiskTech | amlng-web-api | ✅ In Production |
| CATech | sust-pf-admin-service | ✅ In Production |
| AEDT | rest-api-infiniti | ✅ Validation Complete |
| AEDT | lookup-tool-api | 🎯 Sept 12 target |
| FinTech | cill-r2-consumption, waii | ❌ Pipeline type not supported |

> ⚠️ **Finance Tech note:** `cill-r2-consumption` and `waii` repos are flagged as **pipeline type not supported** in Infiniti 2.0. These are active blockers that need escalation to the Infiniti team.

---

## 10. AI-Assisted Monitoring & MCP Guidance

The CICD Pain Points sub-page on AI/MCP guidance addresses three root problems surfaced during ASD badge reviews:

| Problem | Description |
|---------|-------------|
| Missing service/business/infra metrics | Teams use only default Envoy metrics — missing app-specific signals needed for ASD badge |
| Arbitrary thresholds | No baseline established — too aggressive = rollback fatigue, too conservative = missed regressions |
| Broken metrics | When Prometheus data collection breaks, failed metric analysis causes rollbacks that stall healthy changes |

### Three MCP servers to configure in IDE

| MCP Server | Provider | Purpose |
|-----------|----------|---------|
| [Git MCP Server](https://dx.stage.walmart.com/metaregistry/mcp-servers/GECGITHUB-MCP-SERVER) | Strati Developer Enablement | Read repo contents, KITT files |
| [Prometheus MCP Server](https://dx.stage.walmart.com/metaregistry/mcp-servers/MMS_PROMETHEUS_MCP_SERVER) | DP Telemetry | Query Prometheus/MMS metrics |
| [Pipeline MCP Server](https://dx.stage.walmart.com/metaregistry/mcp-servers/PIPELINEMANAGEMENT) | Pipeline Visualizer | Pipeline run data |

> ⚠️ Prometheus MCP Server is currently **Stage only** — not yet promoted to production Meta Registry.

### AI prompts for threshold calibration

**Prompt 1 — Detect spikes during pipeline releases:**
> *"Find the namespace, app name, canary metrics in KITT file of this repository. Use the namespace and the app name to find all spikes in golden signals, WCNP metrics and any other application metrics during non-production pipeline releases. Then match the non-production pipeline release timestamps in UTC with the timestamps in UTC of the spikes and output the answer in a tabular format. Add a column with the value of the metric that spiked and another column showing the configured thresholds for these metrics in KITT file in the repository."*

**Prompt 2 — Baseline thresholds from historical data:**
> *"Find the namespace, app name, canary metrics in non-prod stage in KITT file of this repository. Use the namespace and the app name to establish baselines using past 4 weeks data for golden signals, WCNP metrics and any other application metrics during non-production deployment. Then match the thresholds in canary for non-production with the baselines for non-production and output in a tabular format."*

**What these surface:**
- Memory leaks detected before production (example: Feb 19 release exceeded memory limits by 237% — 5GB actual vs 1.5GB configured)
- Over-conservative CPU thresholds that should be tightened for earlier detection
- Broken Prometheus metric collection that would cause false rollbacks

> **For Finance-eComm data pipelines:** These prompts directly apply to Airflow/Dataproc monitoring. Where Canary analysis isn't available (non-HTTP workloads), these prompts help establish the Prometheus baselines needed for Rollr threshold configuration.

---

## 11. DORA Metrics Targets

Across all levels, DORA metrics progression:

| Metric | Level 4 | Level 5 | Level 6 |
|--------|---------|---------|---------|
| Deployment Frequency | ≥ 5/week | ≥ 5/week | Continuous |
| Development Cycle Time | < 24h | < 24h | < 24h |
| Change Failure Rate | < 15% | < 15% | < 10% |
| MTTR | < 60 min | < 60 min | < 30 min |
| AutoCRQ success rate | > 95% | — | — |
| Rollback incidence | < 1% | — | — |

**DORA is tracked via:** TeamProductivity dashboard (real-time), governance audits (monthly per APM).

---

## 12. Consolidated Action Items & Owners

| # | Action | Owner | Status |
|---|--------|-------|--------|
| 1 | AutoCRQ: Submit EBSASD Jira for ASD Gold badge review | Finance Tech teams | Ongoing |
| 2 | AutoCRQ: Submit RITM with ASD badge # for fast-track | Finance Tech teams | Per APM |
| 3 | ITSM/CAB: Submit to Architect Champions before ITSM to reduce back-and-forth | All teams | Process change |
| 4 | Data pipeline rollback: Engage WCNP Rollr team for custom metrics bridge | AEDT | Open |
| 5 | Rollr beta: Onboard eligible pipelines, define watermark versions | AEDT / FinTech | Aug–Sep 2026 |
| 6 | Rollr automated rollback feature (GA) | GTP Rollr team | STRCONTAIN-36374 (Sep) |
| 7 | Fix WCNP Rollr bugs blocking Level 4 | WCNP Rollr team | PGPTOOLS-331318 |
| 8 | DX portal resource creation APIs (Kafka, WCNP namespace, etc.) | GTP | Q3 FY26 roadmap |
| 9 | Infiniti 2.0: Resolve pipeline type support for `cill-r2-consumption`, `waii` | FinTech + Infiniti team | Active blocker |
| 10 | Infiniti 2.0: Rollr integration | GTP / Infiniti team | Open |
| 11 | Infiniti 2.0: Monorepo support tech strategy | David Callaghan | Open |
| 12 | MMS Prometheus MCP Server: Stage → Prod promotion | MMS team | Pending |
| 13 | Gatekeeper: Move gate validation to before CRQ creation (not after) | GTP / Platform | Known issue |
| 14 | Canary + Rollr: Define guidance for Kafka/ETL workloads | WCNP Rollr team | Open question |
| 15 | ASD badge monthly governance audits | ASD Program team | Monthly cadence |

---

## 13. Key Toolchain Reference

| Tool | Purpose | Mandatory at level |
|------|---------|-------------------|
| **GitHub** | Source control, branch protection (2+ approvers) | Level 1 |
| **Looper + Concord** | Pipeline orchestration, build, test, artifact publish | Level 1 |
| **SonarQube** | Static analysis, quality gates (≥80% coverage, zero blockers) | Level 1–3 |
| **CodeGate** | Security scan on builds | Level 1 |
| **Artifactory** | Artifact repository and versioning | Level 1 |
| **TestHub / TestBurst** | Test metrics aggregation | Level 2 |
| **Golden Signal / MMS / Spotlight** | Monitoring and alerting | Level 2 |
| **Gatekeeper** | Gate enforcement (APM, SSP, Resilient Deployment Pattern) | Level 3 |
| **Stage Gates V2** | Pre/post deploy hook orchestration | Level 3 |
| **Rollr** | 1-click rollback (Level 4+: automated rollback via metrics) | Level 4 |
| **Infiniti 2.0** | All microservices on WCNP — pipeline templating, Gatekeeper before prod | Level 3+ (WCNP) |
| **Snyk** | Dependency vulnerability scanning (no critical CVEs) | Level 3 |
| **TeamProductivity** | DORA metrics dashboard and observability | Level 4 |
| **Semantic versioning** | `Major.Minor.Patch` on all builds — mandatory for traceability | Level 3+ |
| **Prometheus / MMS** | Metrics source for Canary and Rollr threshold evaluation | Level 4 |
| **KITT** | Kubernetes deployment definition (namespace, app, canary config) | Level 3+ (WCNP) |

---

## Appendix: Finance Tech / Data Pipeline — Level by Level Checklist

Based on the pain points documented for AEDT and data pipeline workloads:

| Level | Can Finance-eComm data pipelines achieve this? | Blocker if any |
|-------|-----------------------------------------------|---------------|
| Level 1 | ✅ Achievable | SonarQube + Looper + GitHub already in use |
| Level 2 | ✅ Achievable | Monitoring (Grafana) already in place |
| Level 3 | ✅ Achievable | Requires zero SonarQube blockers + automated DAG deployments via pipeline |
| Level 4 | ⚠️ Partially blocked | Data pipeline metrics not natively connected to Rollr/Canary; WCNP Rollr bugs (PGPTOOLS-331318) |
| Level 5 | ⚠️ Design required | Zero-click deployment for Airflow DAGs needs custom pattern — not standard WCNP rolling update |
| Level 6 | 🔴 Future state | Automated rollback for data pipelines (non-HTTP) not yet a supported pattern |

---

*Sources:*
- *[AutoCRQ Process with ASD](https://confluence.walmart.com/pages/viewpage.action?pageId=3718709954)*
- *[CICD Maturity Levels](https://confluence.walmart.com/pages/viewpage.action?pageId=2646480052)*
- *[CICD Pain Points](https://confluence.walmart.com/pages/viewpage.action?pageId=2689910073) + all 5 sub-pages*
- *[EBS Rollr Initiative](https://confluence.walmart.com/pages/viewpage.action?pageId=2965555459)*
- *[Infiniti 2.0 Adoption](https://confluence.walmart.com/pages/viewpage.action?pageId=2870336697)*
- *[Metrics-based Rollback Strategies](https://confluence.walmart.com/pages/viewpage.action?pageId=2894202164)*
- *[AI-assisted MCP Monitoring Guidance](https://confluence.walmart.com/pages/viewpage.action?pageId=3369397362)*
