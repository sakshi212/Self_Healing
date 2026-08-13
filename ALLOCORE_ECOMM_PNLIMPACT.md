# AlloCore v2 — eComm P&L Impact Analysis & Simulation
### Architecture Design for Channel Hierarchy Changes & New Store Onboarding

> **Source:** Allocation Platform v2 Architecture Design — Confluence FDLH page 3540217374 (Kevin Goslar, June 24 2026)
> **Scope:** This document adapts AlloCore v2's general allocation architecture to the specific eComm use case of channel hierarchy changes and new store onboarding. Sections marked ⬜ *inferred* are reasoned from architecture patterns; everything else is sourced directly from the Confluence page.

---

## Table of Contents

1. [What is AlloCore?](#1-what-is-allocore)
2. [Why This Matters for eComm P&L](#2-why-this-matters-for-ecomm-pl)
3. [The Problem AlloCore Solves](#3-the-problem-allocore-solves)
4. [Workflow Evolution — Current vs Future](#4-workflow-evolution--current-vs-future)
5. [System Architecture — Two Subsystems](#5-system-architecture--two-subsystems)
6. [AI Architecture](#6-ai-architecture)
7. [eComm P&L Use Cases — Full Coverage](#7-ecomm-pl-use-cases--full-coverage)
8. [Functional Requirements Mapped to eComm Use Cases](#8-functional-requirements-mapped-to-ecomm-use-cases)
9. [How Outputs Look — Surface by Surface](#9-how-outputs-look--surface-by-surface)
10. [Channel Hierarchy Change — End-to-End Flow](#10-channel-hierarchy-change--end-to-end-flow)
11. [Non-Functional Requirements](#11-non-functional-requirements)
12. [Integrations](#12-integrations)
13. [Gaps & Open Questions](#13-gaps--open-questions)

---

## 1. What is AlloCore?

**AlloCore** is Walmart's unified financial allocation framework. It allocates shared credits and debits — with business context — across **Walmart Store, Omni, eComm, and Sam's** data.

The core problem it solves: financial data is rarely available at the granularity businesses need.
- Monthly rent exists at the store level → needs allocation per day
- Electricity cost exists at the building level → needs allocation per transaction
- SAP surfaces only aggregated data → shared costs need decomposition to the P&L line

Without AlloCore, every allocation change requires an engineer to write or modify SQL, test in pre-prod, loop with the business for review, and redeploy. **V2 eliminates that loop** by giving business users a self-service AI interface backed by a governed sandbox.

---

## 2. Why This Matters for eComm P&L

Channel hierarchy changes and new store onboarding are one of the most frequent triggers for allocation changes in eComm:

| Business event | Allocation impact |
|----------------|-------------------|
| New fulfillment center goes live | Shared costs (warehouse rent, electricity, staffing) must be re-allocated across the new node. Existing stores' P&L changes even if their volume doesn't. |
| Channel reclassification (1P → WFS) | Commission rates, cost pool memberships, and credit/debit rules all change. Historical P&L attribution shifts. |
| New marketplace seller onboarded | No allocation baseline exists. Refund, commission, and fulfillment cost allocations need to be bootstrapped. |
| Store merged or retired | Allocation rules for the retired node need winding down. Costs redistribute to surviving nodes. |
| Channel split | One allocation bucket becomes two. Historical comparisons break unless the split is back-applied. |

Each of these requires a business user to:
1. Understand what the current allocation logic is
2. Simulate what changes if the hierarchy updates
3. See the P&L impact before it goes live
4. Get it reviewed and approved before production

AlloCore v2 provides the platform to do all four.

---

## 3. The Problem AlloCore Solves

### Current state pain (what makes this hard today)

```
Business user identifies a needed allocation change
          │
          ▼
Submits request to Engineering via ticket / email
          │
          ▼  (days wait)
Engineer interprets intent, writes/modifies SQL
          │
          ▼
Deploy to pre-prod environment for testing
          │
          ▼
Business user reviews output — finds issues / has questions
          │
          ▼  (loop back — can repeat 3–5 times)
Engineer revises SQL
          │
          ▼
Final business approval
          │
          ▼  (days to weeks total)
Production deployment
```

**Result:** Allocation changes that should take hours take days to weeks. Business users have no visibility into the logic. Engineers become a bottleneck for every allocation question.

### Future state (AlloCore v2)

```
Business user asks a question or describes a change
via Conversational UI or self-service Query Editor
          │
          ▼  (minutes)
AI agent interprets intent, generates allocation logic,
runs simulation in sandbox against BQ
          │
          ▼
Business user sees: impact on P&L, affected dependencies,
lineage, before/after comparison — in the UI
          │
          ▼  (hours, not days)
Engineering reviews before production deploy
          │
          ▼
Production deployment via dbt + Helix/Airflow
```

**Result:** Turnaround reduced from days to hours. Business users own the intent; engineering owns the correctness gate.

---

## 4. Workflow Evolution — Current vs Future

AlloCore defines three workflow states:

### Current Workflow
The legacy engineering-heavy process with 6–7 steps looping between business and engineering. Every allocation question or change requires an engineer in the loop from the start. No self-service.

### Future Workflow (V2 — being built now)
AI chat interface + self-service UI for business users. Business creates or modifies allocation logic directly. Engineering reviews the output before it goes to production. Engineering is no longer a first-line resource — they are a final gate.

### Optimized Workflow
Direct "pair programming" style collaboration between financial experts and engineers for complex allocation scenarios. Immediate impact review built into the session — business and engineering work together in real time rather than in a handoff loop.

**For eComm P&L specifically:** the Future Workflow is the target. When a new store is added, the finance analyst runs the simulation themselves, sees the P&L impact, and submits for engineering review — rather than filing a ticket and waiting three days.

---

## 5. System Architecture — Two Subsystems

AlloCore v2 has two distinct systems that serve different purposes:

```
┌─────────────────────────────────────────────────────────────────────┐
│                        ALLOCORE v2                                   │
│                                                                      │
│  ┌────────────────────────────┐  ┌──────────────────────────────┐   │
│  │  PRODUCTION SYSTEM         │  │  IMPACT ANALYSIS &           │   │
│  │                            │  │  SIMULATION SYSTEM           │   │
│  │  dbt workflows on prod     │  │                              │   │
│  │  BigQuery data             │  │  Sandbox BigQuery env        │   │
│  │                            │  │  for safe experimentation    │   │
│  │  Triggered by:             │  │                              │   │
│  │  Helix / Airflow           │  │  Triggered by:               │   │
│  │                            │  │  Business user via UI/chat   │   │
│  │  Who uses it:              │  │                              │   │
│  │  Engineering / scheduled   │  │  Who uses it:                │   │
│  │  pipelines                 │  │  Business users / Finance    │   │
│  │                            │  │  analysts                    │   │
│  │  Output:                   │  │                              │   │
│  │  Production allocation     │  │  Output:                     │   │
│  │  results in BQ             │  │  Simulated P&L impact,       │   │
│  │                            │  │  dependency map, lineage     │   │
│  └────────────────────────────┘  └──────────────────────────────┘   │
│                                                                      │
│  Both run BigQuery SQL on BigQuery tables                            │
│  Both share the same dbt model definitions (prod vs sandbox)         │
└─────────────────────────────────────────────────────────────────────┘
```

### Production System

- Executes dbt workflows against production BigQuery data
- Scheduled and triggered via **Helix** (Walmart's workflow orchestrator) / Airflow
- This is where actual allocation results land — what feeds financial reporting, SAP reconciliation, and downstream P&L tables
- Any change to production goes through the engineering approval gate and is versioned in GitHub

### Impact Analysis & Simulation System

- Runs against a **sandbox BigQuery environment** — no risk to production data
- Business users interact through the Conversational UI or Query Editor
- Simulations are ephemeral: they run on-demand, show impact, and are discarded unless promoted to production
- When a business user is satisfied with the simulation result, they can **request production promotion** — which triggers the engineering review flow

---

## 6. AI Architecture

AlloCore's AI layer is a **multi-agent system** using Pydantic AI with ChatGPT:

```
┌─────────────────────────────────────────────────────────────────┐
│  CONVERSATIONAL UI (chat + UI)                                   │
│  Business user input: natural language question or change request│
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  PRIMARY AGENT                                                   │
│  Intent detection + routing                                      │
│  Powered by: Pydantic AI + ChatGPT                              │
│  Skills: understands allocation domain, BQ schema context        │
└──────┬──────────────┬──────────────┬──────────────┬────────────┘
       │              │              │              │
       ▼              ▼              ▼              ▼
 ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
 │ Impact   │  │ Query    │  │ Lineage  │  │ P&L View │
 │ Analysis │  │ Builder  │  │ Agent    │  │ Agent    │
 │ Sub-agent│  │ Sub-agent│  │ Sub-agent│  │ Sub-agent│
 └─────┬────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘
       │             │             │              │
       └─────────────┴─────────────┴──────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │  MCP Tools             │
              │  (allocation-platform- │
              │   mcp)                 │
              │                        │
              │  Tools expose:         │
              │  · BQ query execution  │
              │  · dbt model access    │
              │  · Sandbox management  │
              │  · GitHub artifact     │
              │    read/write          │
              └────────────┬───────────┘
                           │
                           ▼
                   BigQuery (sandbox)
```

**Key design decision:** Agents delegate to sub-agents as needed. The primary agent does not do BQ execution directly — it routes to the appropriate sub-agent via MCP tool calls. This keeps each sub-agent's scope narrow and testable (100% use-case coverage with assertions on MCP tool calls is the quality bar).

---

## 7. eComm P&L Use Cases — Full Coverage

The following use case matrix maps every scenario for channel hierarchy changes to the AlloCore capability that handles it. This is the "how do we ensure we're capturing all use cases" answer.

### Use Case Categories

**Category A: New Entity Onboarding**

| Use Case | Trigger | AlloCore Capability | Output |
|----------|---------|--------------------|---------| 
| A1 | New store / FC goes live | New store_id appears in hierarchy | Simulation: allocate shared costs to new node; show per-store P&L impact across all existing stores |
| A2 | New marketplace seller onboarded | New seller_id in hierarchy | Simulation: bootstrap commission, refund rate, fulfillment cost allocations from peer sellers |
| A3 | New channel created | New channel_cd in hierarchy | Impact analysis: which allocation rules reference old channel structure; what breaks; what new rules are needed |

**Category B: Reclassification**

| Use Case | Trigger | AlloCore Capability | Output |
|----------|---------|--------------------|---------| 
| B1 | Store re-typed (1P → WFS) | commission_rate changes in hierarchy | Simulation: apply new take rate to store's historical volume; show annualised CM delta |
| B2 | Channel re-bucketed | channel_cd changes for existing store | Dependency map: which downstream dbt models, reports, BQ tables reference this channel_cd; simulate re-attribution |
| B3 | Fulfillment type change | fulfillment_type changes | Simulation: cost pool membership changes; show fulfillment cost reallocation across stores |

**Category C: Retirement / Consolidation**

| Use Case | Trigger | AlloCore Capability | Output |
|----------|---------|--------------------|---------| 
| C1 | Store retired | store_id removed from hierarchy | Impact analysis: where do its allocated costs go; which downstream models reference this store_id |
| C2 | Channels merged | Two channel_cds consolidated | Simulation: merged allocation rules; historical P&L re-stated under merged view |
| C3 | FC consolidated into another | Volume rerouted | Simulation: allocation redistribution; show per-store P&L impact of volume shift |

**Category D: Allocation Rule Changes (without hierarchy change)**

| Use Case | Trigger | AlloCore Capability | Output |
|----------|---------|--------------------|---------| 
| D1 | Commission rate adjustment | Policy change, not hierarchy change | Simulation: apply new rate to current volume; annualised revenue impact |
| D2 | Cost allocation formula change | e.g. rent allocated per sq ft → per transaction | Dependency map + simulation: which stores win/lose under new formula |
| D3 | New cost pool introduced | New shared cost added to the framework | Impact analysis: which nodes absorb new cost; P&L impact per store/channel |

**Category E: Reporting & Audit**

| Use Case | Trigger | AlloCore Capability | Output |
|----------|---------|--------------------|---------| 
| E1 | "Why did this store's CM drop?" | Business question, no pending change | Conversational UI: lineage trace to root allocation change; P&L view with attribution |
| E2 | "What changed in the last 30 days?" | Routine finance review | Workflow lineage view: all allocation changes in period with before/after P&L |
| E3 | Pre-audit evidence package | Compliance / internal audit | Full lineage + versioned SQL from GitHub + approval timestamps |

---

## 8. Functional Requirements Mapped to eComm Use Cases

AlloCore v2 defines 8 functional requirements. This table maps each to the eComm P&L use cases it covers:

| # | FR | Description | Covers use cases |
|---|----|----|---|
| 1 | **Self-Service** | Business users explore metrics, logic, dependencies, simulate changes — no dev team required | All A, B, C, D categories |
| 2 | **Conversational UI** | Chat + UI for questions, impact analysis, P&L reports | A1–A3, E1–E2 |
| 3 | **Collaboration** | Share scenarios with other users | All — analyst shares simulation with Finance VP before Gate 1 approval |
| 4 | **Query Editor** | Direct SQL editing for users comfortable with SQL | B1–B3, D1–D3 for power users |
| 5 | **Impact Analysis & Dependency Mapping** | Simulate before production; show what's affected | A1–C3, D1–D3 |
| 6 | **Workflow Lineage** | Visual lineage of allocation workflow | E2, E3 |
| 7 | **P&L View** | P&L output tied to allocations | All categories |
| 8 | **DataGate Integration** | Integration with Walmart DataGate platform | Governance / data access layer for all |

**Coverage gap check:**
- Category E (Reporting & Audit) relies heavily on FRs 6 (Lineage) and 7 (P&L View) — these are the least specified in the Confluence doc. Lineage and P&L view output format need to be defined before build starts.
- FR 3 (Collaboration) is critical for the two-gate approval flow (business shares with engineering before production promote) but is not elaborated in the Confluence page.

---

## 9. How Outputs Look — Surface by Surface

### Surface 1: Conversational UI (primary)

The chat interface is the main entry point for finance analysts. The conversation drives the simulation — the analyst describes the change in plain English and the AI translates it into allocation logic, runs it in the sandbox, and surfaces the output.

**What an analyst sees for a new store onboarding:**

```
┌────────────────────────────────────────────────────────────────────┐
│  💬 AlloCore Chat                                            [⚙ ▼] │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  You: We're adding store 9876 (WFS fulfillment) on Sept 1.        │
│       What's the P&L impact on our existing stores?               │
│                                                                    │
│  AlloCore:                                                         │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │ Running simulation for store 9876 (WFS) effective Sept 1...  │ │
│  │                                                              │ │
│  │ IMPACT SUMMARY — 13 week projection                          │ │
│  │                                                              │ │
│  │ New store 9876                                               │ │
│  │   Projected GMV:          $4.2M – $6.8M                     │ │
│  │   Projected CM:           $0.3M – $0.9M                     │ │
│  │   Shared cost absorbed:   $120K/quarter (warehouse rent)    │ │
│  │   Break-even week:        Week 9 (base case)                │ │
│  │                                                              │ │
│  │ Impact on existing stores                                    │ │
│  │   Warehouse rent reallocation:  -$8K/quarter per FC         │ │
│  │   (distributed across 15 peer FCs based on sq footage)      │ │
│  │                                                              │ │
│  │ [View full P&L breakdown] [See dependency map]              │ │
│  │ [Share this scenario] [Request production promote]           │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                                                                    │
│  You: Show me the stores most affected by the cost reallocation.  │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

**Key properties of the conversational output:**
- Always shows a range (conservative / base / optimistic) for projections — never a single number
- Shared cost reallocation is shown alongside new store projections — the analyst sees the full system effect, not just the new node
- Every output has actions: drill down, share, promote to production, view lineage
- Follow-up questions are supported within the same session — the AI retains context

---

### Surface 2: P&L View (structured report)

Accessible from the Conversational UI ("View full P&L breakdown") or directly via the UI. Shows the full P&L statement for the simulated allocation state.

```
┌────────────────────────────────────────────────────────────────────────────────┐
│  P&L VIEW — Simulation: Store 9876 onboarding (Sept 1 2026)                   │
│  Scenario: Base  ▼     Period: 13 weeks post-launch    Comparison: vs baseline │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│  VIEW BY:  [Channel ▼]  [Store ▼]  [Fulfillment Type ▼]  [Week ▼]            │
│                                                                                │
│  ┌──────────────────────┬──────────────┬──────────────┬──────────────────┐   │
│  │ P&L Line             │ Baseline     │ Simulated    │ Delta            │   │
│  ├──────────────────────┼──────────────┼──────────────┼──────────────────┤   │
│  │ GMV (all stores)     │ $142.3M      │ $147.8M      │ ▲ +$5.5M (+3.9%) │   │
│  │ ├─ Existing stores   │ $142.3M      │ $141.0M      │ ▼ -$1.3M (-0.9%) │   │  ← shared cost effect
│  │ └─ Store 9876 (new)  │ —            │ $6.8M        │ +$6.8M (new)     │   │
│  ├──────────────────────┼──────────────┼──────────────┼──────────────────┤   │
│  │ Net Revenue          │ $129.4M      │ $134.2M      │ ▲ +$4.8M         │   │
│  │ Refunds              │ -$4.1M       │ -$4.3M       │ ▼ -$0.2M         │   │
│  │ Commission           │ $18.2M       │ $19.1M       │ ▲ +$0.9M         │   │
│  ├──────────────────────┼──────────────┼──────────────┼──────────────────┤   │
│  │ Fulfillment Cost     │ -$31.2M      │ -$32.1M      │ ▼ -$0.9M         │   │
│  │ ├─ Warehouse rent    │ -$8.4M       │ -$8.4M       │ — (reallocated)  │   │  ← allocation visible
│  │ ├─ Last-mile         │ -$14.2M      │ -$14.8M      │ ▼ -$0.6M         │   │
│  │ └─ Handling          │ -$8.6M       │ -$8.9M       │ ▼ -$0.3M         │   │
│  ├──────────────────────┼──────────────┼──────────────┼──────────────────┤   │
│  │ Contribution Margin  │ $98.2M       │ $102.1M      │ ▲ +$3.9M (+4.0%) │   │
│  └──────────────────────┴──────────────┴──────────────┴──────────────────┘   │
│                                                                                │
│  ⚠ Note: Existing store CM decreases $1.3M due to warehouse rent              │
│    reallocation — absorbed by Store 9876. Net system CM is positive.           │
│                                                                                │
│  [Download CSV]  [View by store]  [View lineage]  [Request production promote] │
└────────────────────────────────────────────────────────────────────────────────┘
```

**Key properties of the P&L view:**
- Allocation lines (e.g. warehouse rent) are **visible and labelled** — not hidden inside totals. This is the core differentiator from a plain BQ report.
- **Before / after comparison** is always shown — not just the simulated state
- Existing store impact is shown separately from new store contribution — avoids "hiding" the reallocation effect in aggregate numbers
- Warnings surface automatically when existing stores are adversely affected
- Drillable by any dimension: channel, store, fulfillment type, week

---

### Surface 3: Impact Analysis & Dependency Map

Answers: *"What does this change touch?"* — for both the data model (which dbt models, BQ tables) and the business (which P&L lines, which stores).

```
┌────────────────────────────────────────────────────────────────────────────────┐
│  DEPENDENCY MAP — Store 9876 WFS onboarding                                   │
├───────────────────────────────┬────────────────────────────────────────────────┤
│  DATA IMPACT                  │  BUSINESS IMPACT                               │
│                               │                                                │
│  dbt models affected: 7       │  Stores affected: 16                           │
│  BQ tables affected: 12       │  Channels affected: 2 (1P, WFS)                │
│  Upstream dependencies: 3     │  Allocation pools affected: 4                  │
│                               │                                                │
│  ┌──────────────────────────┐ │  Stores losing allocation share:               │
│  │ alloc_warehouse_rent     │ │  FC-101  -$2.1K/qtr  ▼                        │
│  │   └── pnl_by_store       │ │  FC-102  -$1.9K/qtr  ▼                        │
│  │       └── pnl_by_channel │ │  FC-103  -$2.4K/qtr  ▼                        │
│  │           └── finance_   │ │  FC-104  -$1.8K/qtr  ▼                        │
│  │               reporting  │ │  ... 11 more          ▼                        │
│  │                          │ │                                                │
│  │ alloc_fulfillment_cost   │ │  Stores gaining from new volume:               │
│  │   └── pnl_by_store       │ │  None — store 9876 is net new capacity         │
│  └──────────────────────────┘ │                                                │
│                               │  Allocation pool changes:                      │
│  Missing configs: 1 ⚠         │  WFS_cost_pool  → +1 member                   │
│  store_id 9876 not in         │  WH_rent_pool   → sq footage TBD ⬜            │
│  ETL load params              │                                                │
└───────────────────────────────┴────────────────────────────────────────────────┘
```

**Key properties of the dependency map:**
- Shows **both** data lineage (which dbt models break or change) and business lineage (which stores, channels, pools are affected)
- Flags **missing configs** automatically (store_id not yet in load parameters → would silently drop data post-launch)
- Every affected model is clickable — opens the dbt model definition or the BQ table view
- Designed for the **Engineering approval gate** — eng lead can see exactly what they're approving before they sign off

---

### Surface 4: Workflow Lineage View

Answers: *"What changed, when, who approved it, and what was the P&L effect?"* — primarily for audit, reporting, and retrospective analysis.

```
┌────────────────────────────────────────────────────────────────────────────────┐
│  WORKFLOW LINEAGE — eComm Allocation Changes                                   │
│  Period: Aug 1 – Aug 13 2026   Filter: [Channel: All ▼]  [Status: All ▼]     │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│  ● Sept 1  Store 9876 WFS onboarding                                          │
│    │       Status: SIMULATION  ○──────────────────○ PENDING APPROVAL          │
│    │       Business approval:  Pending (Sakshi S.)                            │
│    │       Engineering approval: Not yet triggered                             │
│    │       Simulated CM impact: +$3.9M / 13 weeks                             │
│    │       [View simulation] [View dependency map]                             │
│    │                                                                           │
│  ● Aug 5   FC-101 reclassified: 1P → WFS                                     │
│    │       Status: ●──────────────────────────────● PRODUCTION                │
│    │       Business approved: Aug 3 (David L.)                                │
│    │       Engineering approved: Aug 4 (Rohan D.)                             │
│    │       Actual CM impact: +$1.2M / 13 weeks  (simulated: +$1.4M, -14%)    │  ← accuracy
│    │       [View simulation vs actual] [View lineage]                          │
│    │                                                                           │
│  ● Jul 20  Warehouse rent formula change (sq ft → per transaction)            │
│            Status: ●──────────────────────────────● PRODUCTION                │
│            Business approved: Jul 18 (Sarah K.)                               │
│            Engineering approved: Jul 19 (Mun C.)                              │
│            Actual CM impact: -$0.4M / 13 weeks (simulated: -$0.3M, +33%)     │  ← flag: large miss
│            ⚠ Simulation accuracy: 67% — deviation review recommended          │
│            [View deviation report] [View lineage]                              │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
```

**Key properties of the lineage view:**
- Every allocation change has a status timeline (simulation → approval → production)
- **Simulation vs actual accuracy** is shown for completed changes — closes the loop
- Large misses are flagged automatically with a recommendation to review the deviation
- Full audit trail: who approved, when, what the SQL was at the time (versioned in GitHub)

---

### Surface 5: Query Editor (power users)

For finance analysts comfortable with SQL — direct access to the allocation logic with real-time sandbox execution.

```
┌────────────────────────────────────────────────────────────────────────────────┐
│  QUERY EDITOR — Sandbox                                          [Run] [Share] │
├────────────────────────────────────────────────────────────────────────────────┤
│  dbt Model: alloc_warehouse_rent                [View lineage] [Compare to prod]│
│  ─────────────────────────────────────────────────────────────────────────────│
│  [EDITOR — shows allocation dbt model SQL, editable]                          │
│                                                                                │
│  Changes from prod version:                                                    │
│  + store_9876 included in WFS_cost_pool                                       │
│  + sq_footage: 42,000                                                          │
│                                                                                │
├────────────────────────────────────────────────────────────────────────────────┤
│  RESULTS (sandbox execution — 2.3s)                                            │
│  ┌────────────┬──────────────┬──────────────┬────────────┐                   │
│  │ store_id   │ allocated_   │ allocated_   │ delta_pct  │                   │
│  │            │ rent_prod    │ rent_sandbox │            │                   │
│  ├────────────┼──────────────┼──────────────┼────────────┤                   │
│  │ 9876 (new) │ —            │ $28,400      │ new        │                   │
│  │ FC-101     │ $31,200      │ $29,800      │ -4.5%      │                   │
│  │ FC-102     │ $28,900      │ $27,600      │ -4.5%      │                   │
│  │ ...        │ ...          │ ...          │ ...        │                   │
│  └────────────┴──────────────┴──────────────┴────────────┘                   │
│                                                                                │
│  [Request production promote]  [View full P&L impact]  [Export]               │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## 10. Channel Hierarchy Change — End-to-End Flow

Combining all surfaces into the full journey for a new store onboarding event:

```
Day 0: Change detected
  CHANNEL_HIERARCHY_MASTER shows new store_id 9876 (WFS)
  AlloCore CDC detects it → creates a simulation job
  Finance analyst receives notification in chat UI
         │
         ▼
Day 0–1: Simulation (Conversational UI)
  Analyst opens AlloCore chat
  Describes intent: "new WFS store, Sept 1 launch"
  AI agent runs simulation in sandbox BQ
  Analyst sees: P&L impact summary + dependency map
  Analyst asks follow-up questions, adjusts assumptions
  Analyst satisfied → clicks [Share with team]
         │
         ▼
Day 1–2: Gate 1 — Business Approval
  Analyst shares simulation with Finance VP via [Share scenario]
  VP sees: P&L view (3 scenarios) + AI narrative + lineage
  VP approves via UI or email link
         │
         ▼
Day 2–3: Gate 2 — Engineering Approval
  Engineering lead receives dependency map + missing config alerts
  Eng confirms: store_id 9876 added to ETL_LOAD_PARAMETERS
  Eng approves via UI
         │
         ▼
Day 3: Production Promote
  AlloCore creates PR in GitHub with dbt model changes
  PR passes automated tests (90%+ coverage) + linters
  Engineering merges
  Helix / Airflow deploys dbt models to production
         │
         ▼
Week 2–14: Post-launch tracking
  AlloCore compares actual P&L to simulation week by week
  Lineage view shows accuracy score
  If >20% miss: deviation report surfaces in lineage view
```

**Total turnaround: 3 days** (vs weeks in the current workflow)

---

## 11. Non-Functional Requirements

| NFR | Target | How AlloCore addresses it |
|-----|--------|--------------------------|
| **Latency** | Simulation in hours, not days | Sandbox BQ execution on-demand; AI agent generates allocation logic in minutes |
| **Governance & Compliance** | Versioned, immutable, full change tracking | GitHub as source of truth; every SQL change versioned; approval timestamps recorded |
| **Correctness** | Test SQL before production | Sandbox environment; 90%+ unit test coverage; dbt tests on all models |
| **Transparency** | Full data lineage, clear calculation explanations | dbt lineage surfaced in UI; AI explains allocation logic in plain English |
| **Consistency** | Single unified allocation framework | One AlloCore platform for Store, Omni, eComm, Sam's — no parallel allocations |
| **Cost Efficiency** | Lower headcount + cloud cost | Self-service removes engineering from every allocation question; query optimization in dbt |
| **Quality bar** | ≥ 90% unit test coverage; 100% AI eval coverage | Automated evals with assertions on MCP tool calls |

---

## 12. Integrations

| System | Role in eComm P&L context |
|--------|--------------------------|
| **BigQuery** | Both systems (Production + Simulation) run all allocation SQL here. eComm tables (WM_SALES_ORDER_INV_CHRG_DTL, CHANNEL_HIERARCHY_MASTER) are the primary inputs |
| **dbt** | Modular, testable SQL models for allocation logic. Provides lineage natively. Sandbox = same models, different BQ dataset |
| **GitHub** | Source of truth for all dbt model SQL, tests, and documentation. Every allocation change creates a PR. Full version history = audit trail |
| **Helix / Airflow** | Triggers production dbt runs. After engineering approves and merges the PR, Helix picks up the updated models for the next scheduled run |
| **SSO / PingFed + AD** | Role-based access: Finance analysts can simulate; only Engineering can merge to production |
| **WCNP** | Cloud runtime for the AlloCore application (API + frontend). Monitoring and alerting via WCNP standard stack |
| **DataGate** | Governance layer — controls which BQ datasets and tables are accessible to the simulation sandbox |
| **AI Services (ChatGPT)** | Powers the Conversational UI. Intent detection, allocation logic generation, plain-English explanations |
| **MCP (allocation-platform-mcp)** | Tool layer connecting AI agents to BQ execution, dbt model access, sandbox management, GitHub |

---

## 13. Gaps & Open Questions

| # | Gap | Impact | Owner |
|---|-----|--------|-------|
| 1 | **P&L view output format not specified** in Confluence | Medium — wireframe above is inferred from FRs, needs UX validation | UX / Product |
| 2 | **Simulation output format not specified** | Medium — dependency map and P&L delta structure need design spec | UX / Product |
| 3 | **Fulfillment cost source** — which BQ table feeds into allocation? | High — needed before dbt models can be written | Engineering |
| 4 | **Diagram contents** — 7 draw.io diagrams in Confluence are not text-accessible | High — System Overview, Detailed View, Cloud, AI Architecture diagrams contain critical detail | Access via Confluence directly |
| 5 | **MCP API details** — allocation-platform-mcp API doc requires GitHub auth | Medium — tool surface for AI agents is partially defined | Access via GEC GitHub after re-auth |
| 6 | **FR 3 (Collaboration)** — sharing mechanism not elaborated | Low — needed for Gate 1 approval flow (analyst shares sim with VP) | Product |
| 7 | **eComm-specific allocation rules** — what are the current dbt models for eComm allocations? | High — needed to understand what changes when hierarchy updates | Engineering / FDL team |

---

*Source: AlloCore Allocation Platform v2 Architecture Design — Confluence FDLH page 3540217374*
*Platform: GCP BigQuery · dbt · Pydantic AI + ChatGPT · FastAPI · React (Living Design) · WCNP · GitHub*
