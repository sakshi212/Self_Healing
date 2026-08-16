# Architecture Review Board — Engineering Guardrails & Best Practices
### Generic Reference for All Engineering Solutions · Walmart Finance Tech / AEDT / EBS

> **Purpose:** This document defines the engineering guardrails, standards, and best practices that every solution must meet before ARB approval and production deployment. It is a living reference — applicable to any system: APIs, data pipelines, AI/ML platforms, microservices, batch jobs, or infrastructure.
>
> **Use this document to:**
> - Self-assess any solution before submitting to ARB
> - Ensure all guardrails are in place during design and build
> - Onboard new engineers to the engineering excellence bar
> - Reference specific Walmart standards (EDEE ARB, ASD, AutoCRQ, DORA)

---

## Table of Contents

1. [Architecture Review Checklist](#1-architecture-review-checklist)
2. [Security Guardrails](#2-security-guardrails)
3. [CI/CD Standards & ASD Maturity](#3-cicd-standards--asd-maturity)
4. [Observability Standards](#4-observability-standards)
5. [Reliability & Resilience Patterns](#5-reliability--resilience-patterns)
6. [Data Quality & Governance](#6-data-quality--governance)
7. [Cost Governance](#7-cost-governance)
8. [Multi-Tenancy & Market Isolation](#8-multi-tenancy--market-isolation)
9. [Non-Functional Requirements Template](#9-non-functional-requirements-template)
10. [Failure Mode Analysis Framework](#10-failure-mode-analysis-framework)
11. [Technology Choice Standards](#11-technology-choice-standards)
12. [Delivery & Traceability Standards](#12-delivery--traceability-standards)
13. [ARB Submission Checklist](#13-arb-submission-checklist)

---

## 1. Architecture Review Checklist

Use this checklist during architecture design — before writing any code. Every item must be addressed in the ARB High Level Design (HLD) document.

### Problem & Value

- [ ] Problem statement is clearly defined with quantifiable business impact
- [ ] ROI is defined with measurable key results (not just qualitative benefits)
- [ ] Business use cases are testable: every use case has a condition → expected outcome → verifiable assertion
- [ ] Functional requirements are testable: every requirement has a specific pass/fail criterion

### System Design

- [ ] System boundaries are clearly defined — no domain or sub-domain boundary violations
- [ ] Architectural style is appropriate (event-driven, microservices, batch, streaming) and justified
- [ ] Service boundaries are well-defined: high cohesion, low coupling
- [ ] Reuse of existing components is evaluated before building new ones
- [ ] All data flows and sequence diagrams are documented

### Operational Readiness

- [ ] All failure modes are identified with mitigation strategy per mode
- [ ] Rollback strategy is defined for every deployment type
- [ ] Idempotency is designed into every operation that can be retried
- [ ] Recovery path from any failure is deterministic and documented
- [ ] On-call runbooks exist for every alert type

### Standards Compliance

- [ ] GTP technology policy compliance verified for all technology choices
- [ ] All API contracts documented with OpenAPI spec
- [ ] AuthN/AuthZ specified for every interface (UI, API, programmatic)
- [ ] CI/CD pipeline meets minimum ASD Level 3 criteria (see §3)
- [ ] Observability meets Golden Signal standards (see §4)
- [ ] NFRs are SMART-formatted and testable (see §9)

---

## 2. Security Guardrails

Every solution must satisfy all of the following before ARB approval. Security is not an afterthought — it must be designed in from the start.

### Pre-build requirements

| Guardrail | Standard | Enforcement |
|-----------|---------|-------------|
| **APM ID** | Every solution has an Application Portfolio Management ID | Gatekeeper blocks deployments without APM tagging |
| **SSP filed** | Solution Security Plan submitted and approved | SSP# must appear in the HLD before ARB submission |
| **Threat model** | All attack surfaces identified with mitigations | Included in SSP |

### Code-level security

| Guardrail | Standard | Tool |
|-----------|---------|------|
| **Static analysis** | Zero Blocker findings (SonarQube) | SonarQube — Level 3 hard requirement |
| **Dependency scanning** | No critical or high CVEs | Snyk — blocking gate in CI/CD |
| **Secret detection** | No credentials, tokens, or API keys in code or diffs | CodeGate — runs on every build |
| **Code coverage** | ≥ 80% for new code | SonarQube — Level 2 requirement |
| **License compliance** | No GPL or other license violations in dependencies | Snyk |

### Infrastructure & network security

| Guardrail | Requirement |
|-----------|-------------|
| **Network boundaries** | Architecture diagram must show all network zones, firewall crossings, ports, protocols, and direction of connection instantiation |
| **Private endpoints** | All cloud services (GCP, Azure) accessed via private endpoints — no public internet exposure |
| **WCNP compliance** | Containerized workloads on WCNP with correct namespace isolation |
| **Secrets management** | All secrets in Vault / Akeyless — never in environment variables, config files, or code |
| **TLS everywhere** | All service-to-service communication encrypted in transit |
| **Least privilege** | Service accounts scoped to minimum required permissions — reviewed at every ARB |

### Data security

| Guardrail | Requirement |
|-----------|-------------|
| **PII handling** | PII identified at design time; encryption at rest and in transit specified; access controls defined |
| **Data classification** | All data assets classified (Public / Internal / Confidential / Restricted) |
| **Regulatory compliance** | Applicable standards explicitly acknowledged: SOX, PCI DSS, GDPR, CCPA, HIPAA (see checklist in §13) |
| **Data residency** | Any data leaving Walmart's network or sent to external APIs (including LLMs) must be reviewed with Security |

### SSP architecture diagram requirements

The architecture diagram attached to the SSP must clearly identify:
- Network boundaries (Walmart, cloud, vendor, internet, DMZ)
- All systems: web servers, app servers, databases, integrations
- Every port, protocol, and direction of connection
- New vs. existing components (new must be bolded/clearly marked)
- Data flows with sensitivity classification

---

## 3. CI/CD Standards & ASD Maturity

All homegrown solutions must achieve minimum **ASD Level 3** before production deployment. Level 4+ is the FY27 target for all EBS APMs.

### Minimum requirements by level

| Level | When required | Key requirements |
|-------|--------------|-----------------|
| **Level 1** | All new repos | GitHub, 2+ PR approvers, SonarQube, Looper+Concord, CodeGate |
| **Level 2** | Before QA promotion | SonarQube PR decoration, ≥80% coverage, integration tests, Golden Signal monitoring |
| **Level 3** | Before production | Zero SonarQube blockers, E2E tests gate, no manual production changes, **AutoCRQ eligible** |
| **Level 4** | FY27 target | Performance testing, smoke tests, Auto CRQ enabled, DORA metrics tracked |

### CI/CD pipeline non-negotiables

```
Every pipeline must have:
  ✓ Semantic versioning (Major.Minor.Patch) on all builds
  ✓ Build artifacts published to Artifactory
  ✓ SonarQube scan on every PR (zero blockers before merge)
  ✓ Snyk dependency scan on every build
  ✓ CodeGate secrets scan on every build
  ✓ Unit tests with coverage report
  ✓ Integration tests in QA environment
  ✓ Automated smoke tests on production deployment
  ✓ Rollback mechanism defined (1-click minimum; automated at Level 4+)
  ✓ PRs require 2+ approvers before merge to main
  ✓ Branch protection on main (no direct pushes)
```

### AutoCRQ eligibility

AutoCRQ (automated CRQ generation and approval) is available only for:
- Solutions with ASD Gold (Level 3) badge
- **Operational changes only:** credential rotations, certificate renewals, vault key updates, runtime config changes
- **Not eligible:** code fixes, feature changes, library upgrades, schema changes

Branch naming convention for AutoCRQ: `/autocrq_<JIRA#>`

**AutoCRQ fast-track path:**
1. Achieve ASD Level 3 badge (EBSASD Jira ticket)
2. Submit RITM with badge # → fast-tracked to VP + CAB
3. CI/CD gate validates branch name + LLM PR scan + JIRA status = Done
4. CRQ auto-generated and auto-approved

### DORA targets

| Metric | Level 4 target | Level 5+ target |
|--------|---------------|----------------|
| Deployment frequency | ≥ 5/week | Continuous |
| Development cycle time | < 24 hours | < 24 hours |
| Change failure rate | ≤ 15% | ≤ 10% |
| MTTR | ≤ 60 minutes | ≤ 30 minutes |

---

## 4. Observability Standards

Every production system must emit the four Golden Signals and have alerting defined before go-live.

### Golden Signals (mandatory for all solutions)

| Signal | What to measure | Alert threshold approach |
|--------|----------------|------------------------|
| **Latency** | P50, P95, P99 response time | Baseline from last 4 weeks; alert on 2× P99 deviation |
| **Traffic** | Requests per second / events per minute | Alert on unexpected drop (upstream failure) or spike (load event) |
| **Errors** | Error rate (5xx for APIs; task failure rate for pipelines; DLQ depth for streaming) | Alert when error rate > X% sustained for Y minutes |
| **Saturation** | CPU, memory, disk, queue depth | Alert at 80% capacity sustained |

### Monitoring toolchain

| Use case | Tool |
|----------|------|
| Infrastructure and application metrics | MMS (Prometheus-compatible) + Spotlight |
| Dashboards and visualization | Grafana |
| Alerting | Golden Signal / MMS alerts |
| Log aggregation | GCP Stackdriver / Azure Monitor |
| Pipeline health (data engineering) | Airflow UI + GCS logging + Grafana |
| DORA metrics | TeamProductivity dashboard |
| On-call alerting (P0/P1) | PagerDuty |
| Team alerting (P2/P3) | Slack alert channels |

### Alert routing standards

| Severity | Criteria | Route to |
|----------|----------|----------|
| **P0** | Data corruption, production down, SLA breach | PagerDuty + Slack |
| **P1** | Degraded availability, processing lag > SLA | PagerDuty + Slack |
| **P2** | Performance degradation, capacity warning | Slack alert channel |
| **P3** | Trend anomaly, efficiency degradation | Daily digest |

### Threshold calibration (before go-live)

Do not use default or arbitrary thresholds. Calibrate against 4 weeks of baseline data:
1. Identify key metrics in KITT file (namespace, app name, canary metrics)
2. Query Prometheus/MMS for baseline during last 4 weeks of non-prod deployments
3. Set alert thresholds at 2× the observed P99 (avoid rollback fatigue from too-aggressive thresholds)
4. Validate thresholds against known incident timestamps

> **Anti-pattern:** Arbitrary thresholds are one of the top three reasons ASD badge reviews fail. No baseline = threshold fatigue = engineers ignoring alerts = missed incidents.

---

## 5. Reliability & Resilience Patterns

### Mandatory patterns

| Pattern | Requirement | Notes |
|---------|-------------|-------|
| **Retry with backoff** | All external calls have retry with exponential backoff and max attempts | Never retry indefinitely — circuit breaker required |
| **Circuit breaker** | Maximum auto-retry attempts per time window | Prevents cascading failure from one misbehaving dependency |
| **Idempotency** | Every operation that can be retried produces the same result when re-run | Critical for pipelines, event consumers, state-modifying operations |
| **Timeout** | All external calls have explicit timeouts | No unbounded waits — every call must have a deadline |
| **Graceful degradation** | System continues serving reduced functionality when a dependency is unavailable | Define the degraded mode explicitly at design time |
| **Rollback** | Every deployment has a tested rollback path | 1-click minimum (Rollr); automated rollback at Level 4 (metrics-triggered) |

### Idempotency checklist (particularly important for data systems)

| Operation type | Idempotent pattern |
|---------------|-------------------|
| BQ table writes | `WRITE_TRUNCATE` or `MERGE` — never `WRITE_APPEND` without deduplication |
| Kafka consumer processing | Deduplication key on `(event_id, event_ts, source_id)` |
| Hudi / Delta table writes | Upsert by primary key (CoW) — re-running produces same result |
| REST API calls | Idempotency key header on all state-changing operations |
| File/GCS writes | Overwrite mode — never append without explicit dedup step |
| Database updates | Upsert patterns (`INSERT ... ON CONFLICT DO UPDATE`) |

### High availability requirements

| Tier | Min replicas | Failover | Notes |
|------|-------------|---------|-------|
| P0 critical service | ≥ 3 | Automatic | Multi-zone deployment mandatory |
| P1 important service | ≥ 2 | Automatic | At least Level 6 ASD for production |
| P2 batch / pipeline | ≥ 1 + retry | Manual or automated | Airflow retry + idempotency covers most cases |

### Blast radius controls

Before deploying any change, define:
1. **What breaks if this fails?** — identify all downstream consumers
2. **How do we contain it?** — circuit breakers, feature flags, pause mechanisms
3. **How do we recover?** — rollback path, replay from source, manual intervention steps
4. **Who do we notify?** — escalation chain with response SLAs

---

## 6. Data Quality & Governance

### Data contract validation

All data systems must validate data at the point of ingestion — not at the point of consumption. Bad data must be caught before it propagates:

| Validation type | What it checks | On failure |
|----------------|---------------|-----------|
| **Schema compatibility** | Message matches registered schema version; no unannounced field changes | Route to DLQ; alert |
| **Mandatory attributes** | Required fields present and non-null | Route to DLQ; alert |
| **Business rules** | Value ranges valid, referential integrity, format rules | Route to DLQ; alert |
| **Volume anomaly** | Record count within expected range vs. prior period | Alert; do not drop |

### Dead Letter Queue (DLQ) standards

- Every ingestion path must have a DLQ
- Failed records preserved with: original payload, failure reason, timestamp, source metadata
- DLQ depth is a P1 alert metric (data being silently dropped)
- DLQ records are replayable — no permanent loss
- DLQ reviewed and cleared within SLA

### Great Expectations / data quality testing

For data engineering solutions:

| Gate | When it runs | What it validates |
|------|-------------|------------------|
| **Pre-deploy** | On every PR in CI/CD | Schema expectations for all output datasets — column types, constraints, value ranges |
| **Post-deploy smoke** | After production deployment | Row count vs prior day, null rates, metric value ranges |

Expectation suites must be version-controlled alongside pipeline code — not managed separately.

### Medallion / data layer standards

| Layer | Purpose | Standards |
|-------|---------|-----------|
| **Bronze (Raw)** | Immutable source of truth | Never transform, never delete; partitioned by date/source; replayable |
| **Silver (Cleansed)** | Trusted, deduplicated, business-rule-applied | Dedup key enforced; reconciliation against source documented |
| **Gold (Curated)** | Business-ready aggregates and KPIs | Single definition per metric; no divergent calculations across teams |

---

## 7. Cost Governance

### Design-time cost requirements

Every ARB submission must include:

| Section | What to document |
|---------|-----------------|
| **Infrastructure cost estimate** | Monthly cost by component (compute, storage, networking, managed services, LLM tokens) |
| **Cost model** | Fixed vs. variable; what drives cost at scale |
| **Cost at peak** | Cost estimate during Black Friday / peak event (not just average) |
| **Optimization opportunities** | Identified before build, not after |

### Compute cost controls

| Control | Standard |
|---------|---------|
| **Autoscaling** | All compute must autoscale — no static provisioning for peak |
| **Ephemeral clusters** | Batch workloads use ephemeral clusters (created per run, deleted on completion) |
| **Right-sizing** | Weekly utilization review; downsize clusters consistently below 40% CPU |
| **Idle compute detection** | Alert on compute idle > 30 minutes during non-scheduled windows |
| **Compute tiering** | Match compute to workload: serverless for light SQL, ephemeral for heavy batch, persistent for streaming |

### Storage cost controls

| Control | Standard |
|---------|---------|
| **Lifecycle rules** | GCS/blob storage: hot → nearline → coldline → delete based on access frequency and retention policy |
| **Compression** | All cold storage in Parquet (compressed) or equivalent |
| **Partition pruning** | BQ and GCS partitioned to minimize bytes scanned per query |
| **Retention policies** | Defined per data domain; compliance-driven retention documented |

---

## 8. Multi-Tenancy & Market Isolation

### Deployment isolation (mandatory for multi-market solutions)

| Isolation requirement | Standard |
|-----------------------|---------|
| **Market-level CI/CD** | Changes scoped and deployed per market independently — a US deployment failure must not block Canada |
| **Blast radius containment** | Feature flag or configuration controls which markets are affected by any change |
| **Independent rollback** | Rolling back a Canadian change must not affect US pipeline state |
| **Test suite scoping** | Automated tests run only for changed markets — not a full regression for every deployment |

### Multi-tenancy certification checklist

- [ ] Multi-market support defined (US, Canada, Mexico, etc. as applicable)
- [ ] Multi-environment isolation (dev / QA / staging / prod) with no shared state
- [ ] Feature flags configurable per tenant / market / policy file — not hardcoded
- [ ] Error messages and UI labels support multi-locale (if customer-facing)
- [ ] APM IDs assigned per market segment if billing isolation is required
- [ ] N+1 market addition does not require code change — only configuration

### Environment standards

| Environment | Purpose | Deployment trigger |
|-------------|---------|-------------------|
| **Dev** | Developer testing, unit tests | Auto-deploy on merge to develop branch |
| **QA** | Integration and E2E testing | Auto-deploy; E2E gate before promotion |
| **Staging** | Production mirror; final validation | Auto-deploy; smoke tests before promotion |
| **Production** | Live | Automated (Level 4+) or manual approval gate |

---

## 9. Non-Functional Requirements Template

All NFRs must be **SMART and testable**: Metric → Trigger → Operations Context → Component → Response → Response Measure → Time Scope.

### Required NFR categories

**Availability & Recovery**

| NFR | Target | Notes |
|-----|--------|-------|
| RTO (Recovery Time Objective) | Define per tier | Time from failure to restoration in alternate region |
| RPO (Recovery Point Objective) | Define per tier | Maximum acceptable data loss |
| Availability % | ≥ 99.9% for P0 systems | Measured monthly |
| Scheduled downtime | < X minutes per window | Define maintenance windows |

**Scalability**

| NFR | Normal | Peak |
|-----|--------|------|
| Transactions per second | XXX TPS | XXX TPS |
| API requests per second | XXX RPS | XXX RPS |
| Concurrent users / connections | XXX | XXX |
| Data volume growth (QoQ) | Define | Define |

**Performance**

| NFR | P95 | P99 |
|-----|-----|-----|
| API response time | Y ms | Z ms |
| Pipeline processing lag | Y min | Z min |
| Query response time | Y sec | Z sec |

**Regulatory compliance checklist**

| Standard | Applicable? | Requirement |
|----------|------------|-------------|
| SOX | ✅ Finance Tech | Audit trail for all data changes; change control; access logging |
| PCI DSS | ✅ Payment data | Cardholder data encryption; access controls; audit logs |
| GDPR | ✅ EU customer data | Data subject rights; consent; deletion capability |
| CCPA / CRPA | ✅ CA customer data | Opt-out mechanism; data inventory |
| HIPAA / HITECH | Only if health data | — |
| COPPA | Only if minors | — |

---

## 10. Failure Mode Analysis Framework

Every ARB submission must include a failure mode table. Use this template:

| Failure Mode | What triggers it | Impact (High/Med/Low) | Mitigation | Recovery path | Owner |
|-------------|-----------------|----------------------|-----------|--------------|-------|
| Dependency unavailable | External API / database down | High | Circuit breaker + retry | Fallback response or graceful degradation | Service team |
| Cascading failure | Upstream failure propagates | High | Downstream consumers paused; blast radius identified | Resume in dependency order after root fix | Platform |
| Data corruption | Bad data written to production | High | Immutable source layer; DLQ; validation at ingestion | Replay from immutable source (Bronze) | Data eng |
| Auth failure (token/cert expired) | Credential rotation missed | Medium | Alert on expiry approach (30 days before); automated rotation where supported | Manual rotation by ops (not auto-fixable) | SecOps |
| OOM / resource exhaustion | Volume spike exceeds provisioned capacity | Medium | Autoscaling; circuit breaker on retry | Scale up parameters; retry with new config | Infra |
| Deployment failure | Bad code pushed to production | High | Smoke tests; automated rollback | 1-click rollback to last stable (Rollr/Infiniti) | Engineering |
| Schema drift | Upstream producer changes schema | Medium | Schema registry; contract tests; DLQ on violation | Fix downstream consumer; replay from DLQ | Data eng |
| *(add rows for every identified failure mode)* | | | | | |

### Failure mode analysis requirements

- Every identified failure mode must have a mitigation and recovery path
- Mitigation must be implemented — not "planned for future sprint"
- Recovery path must be tested in staging before production deployment
- On-call runbook must exist for every High-impact failure mode

---

## 11. Technology Choice Standards

### GTP (Global Technology Platform) compliance

All technology choices must be from the GTP-approved list. Non-standard technology requires an exception process:
1. Document why approved alternatives don't meet the requirements
2. Submit technology exception to Architecture board
3. Get GTP approval before build begins — not after

### Database selection guidance

| Use case | Approved choice | Why |
|----------|----------------|-----|
| Transactional / OLTP | Cloud SQL (PostgreSQL/MySQL MSO) | ACID, managed, WCNP-native |
| Analytical / OLAP | BigQuery | Serverless, massive scale, no cluster management |
| Graph relationships | Neo4j (exception required) or alternative | Graph traversal for lineage and relationships |
| Time-series metrics | Prometheus / MMS | Golden Signal standard |
| Caching | Redis (GTP-approved managed) | Low-latency key-value |
| Event streaming | Kafka (Walmart managed) | Standard for all event-driven architectures |
| Object storage | GCS / Azure Blob | Multi-market, cost-effective, lifecycle rules |
| Mutable data lake | Apache Hudi (CoW) / Delta Lake | ACID on data lake; point-in-time recovery |

### Messaging and streaming

- Kafka is the standard for all event-driven architectures
- Consumers must be idempotent (at-least-once delivery guarantee)
- DLQ required on every consumer
- Schema registry required for all Kafka topics in production

### AI / LLM usage standards

| Requirement | Standard |
|-------------|---------|
| **Data residency** | Confirm with Security before sending any data to external LLM APIs |
| **PII** | Strip all PII from prompts/context before sending to LLM |
| **Structured output** | Use Pydantic or equivalent schema validation — never consume free-text LLM output in automation |
| **Confidence scoring** | Every LLM recommendation must include a confidence score; define thresholds for auto-execution vs human review |
| **Human approval gate** | No LLM-generated change reaches production without human review and approval |
| **Hallucination prevention** | Ground every recommendation in cited evidence (log signals, retrieved data, tool outputs) — not LLM-generated assertions |
| **Cost tracking** | LLM token usage tracked per day; alert on cost anomalies |

### CI/CD toolchain (mandatory for all WCNP solutions)

| Tool | Purpose | Required at |
|------|---------|------------|
| GitHub | Source control | Level 1 |
| Looper + Concord | Pipeline orchestration | Level 1 |
| SonarQube | Static analysis | Level 1 |
| Snyk | Dependency scanning | Level 3 |
| CodeGate | Secret scanning | Level 1 |
| Artifactory | Artifact repository | Level 1 |
| Gatekeeper | Gate enforcement | Level 3 |
| Rollr | Rollback mechanism | Level 4 |
| Infiniti 2.0 | Pipeline templating (WCNP microservices) | Level 3 (WCNP) |
| Semantic versioning | Build traceability | Level 3 |

---

## 12. Delivery & Traceability Standards

### Semantic versioning

All builds must be version-tagged: `Major.Minor.Patch`
- **Major:** Breaking changes
- **Minor:** New features, backward compatible
- **Patch:** Bug fixes, operational changes

No deployment proceeds without a version tag. Untagged builds are rejected by Gatekeeper.

### Requirement traceability

Every business requirement must trace to a specific solution component and its test. This is a hard ARB requirement:

| Business Requirement | Solution Component | Functional Requirement | Test |
|---------------------|-------------------|----------------------|------|
| *(example)* Detect failures within 5 minutes | Monitoring + alerting | FR: alert fires within 5 min of anomaly | Load test + synthetic failure injection |
| *(add rows for every business requirement)* | | | |

### Change governance

| Change type | Process | Timeline |
|-------------|---------|---------|
| Code / logic change | Full CI/CD pipeline + ASD review | Per DORA targets |
| Operational change (creds, certs, config) | AutoCRQ (if Level 3+ badge) | Hours |
| Infrastructure / schema change | Full ASD review | Weeks |
| Architecture change | ARB HLD submission + review | Weeks |

### Tech debt register

Every ARB submission must document new tech debt introduced:

| Tech debt item | Why introduced | Plan to address | Target milestone |
|---------------|---------------|----------------|-----------------|
| *(document every known shortcut or compromise)* | | | |

---

## 13. ARB Submission Checklist

Complete all items before submitting HLD to the Architecture Review Board.

### Documentation artifacts

- [ ] HLD document following EDEE ARB template (all 19 sections complete)
- [ ] APM ID obtained
- [ ] SSP filed and SSP# referenced in HLD
- [ ] Architecture diagram (draw.io in Confluence or GitHub Enterprise) with:
  - [ ] All network boundaries and firewall crossings
  - [ ] All systems with ports, protocols, connection direction
  - [ ] New components clearly bolded/marked
  - [ ] Data flows with sensitivity classification
- [ ] NFR document filed (DX NFR Template format)
- [ ] Data flow / sequence diagrams for all key flows

### Quality gates

- [ ] SonarQube: zero Blocker findings on entire codebase
- [ ] Snyk: no critical or high CVEs
- [ ] Code coverage: ≥ 80%
- [ ] All unit tests passing
- [ ] Integration tests in QA passing
- [ ] E2E tests gate enforced before environment promotion

### Security

- [ ] Threat model completed
- [ ] PII fields identified and handling documented
- [ ] Secrets management via Vault/Akeyless (no secrets in code)
- [ ] Regulatory compliance checklist completed (SOX, PCI DSS, GDPR, CCPA as applicable)
- [ ] Data residency confirmed for any external API calls (including LLMs)

### Operational readiness

- [ ] Golden Signal monitoring configured for all production components
- [ ] Alert thresholds calibrated from baseline data (not arbitrary)
- [ ] On-call runbooks exist for every High-severity failure mode
- [ ] Rollback tested in staging environment
- [ ] Smoke tests automated for production deployment
- [ ] DLQ configured for all data ingestion paths

### Governance

- [ ] Multi-tenancy certification completed
- [ ] Tech debt register included
- [ ] Requirement traceability matrix completed
- [ ] Discovery track open questions have assigned owners
- [ ] LOE estimate reviewed and signed off by Tech Lead and PM
- [ ] DORA metrics baseline established (for Level 4+ ASD targets)

### Pre-submission tip

Submit to **Architect Champions** before ITSM/CAB to reduce back-and-forth. CAB reviews only happen on alternate days — a poorly prepared submission can add a month to the timeline. Use this checklist to ensure the HLD is complete before engaging the ARB.

---

## Quick Reference Links

| Resource | Link |
|----------|------|
| EDEE ARB Template | [confluence.walmart.com/display/EDEE/Architecture+Design+Template](https://confluence.walmart.com/display/EDEE/Architecture+Design+Template) |
| Architecture Review Process | [dx.walmart.com/guides/dx/Architecture-Review-Process-D0000002242](https://dx.walmart.com/guides/dx/Architecture-Review-Process-D0000002242) |
| NFR Template | [dx.walmart.com/guides/dx/Non-Functional-Requirements-Template-D8j1fr48j52](https://dx.walmart.com/guides/dx/Non-Functional-Requirements-Template-D8j1fr48j52) |
| ASD Guidelines | [confluence.walmart.com/pages/viewpage.action?pageId=2656902266](https://confluence.walmart.com/pages/viewpage.action?pageId=2656902266) |
| AutoCRQ Process | [confluence.walmart.com/pages/viewpage.action?pageId=3718709954](https://confluence.walmart.com/pages/viewpage.action?pageId=3718709954) |
| CI/CD Maturity Levels | [confluence.walmart.com/pages/viewpage.action?pageId=2646480052](https://confluence.walmart.com/pages/viewpage.action?pageId=2646480052) |
| CI/CD Pain Points | [confluence.walmart.com/pages/viewpage.action?pageId=2689910073](https://confluence.walmart.com/pages/viewpage.action?pageId=2689910073) |
| Rollback Strategy Guide | [confluence.walmart.com/pages/viewpage.action?pageId=2894202164](https://confluence.walmart.com/pages/viewpage.action?pageId=2894202164) |
| Infiniti 2.0 Adoption | [confluence.walmart.com/pages/viewpage.action?pageId=2870336697](https://confluence.walmart.com/pages/viewpage.action?pageId=2870336697) |
| Getting Started with APM and SSP | [dx.walmart.com/guides/dx/Getting-started-with-APM-and-SSP-Dm26sghevr4](https://dx.walmart.com/guides/dx/Getting-started-with-APM-and-SSP-Dm26sghevr4) |
