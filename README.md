# Databricks Workload Optimizer

Personal diagnostic toolkit for Databricks. Surfaces slow jobs, expensive queries, misconfigured clusters, and table health issues — and tells you exactly what to do about them in plain English.

Built for individual developers and vibe coders who don't have a data engineering background and don't know why their pipelines are slow or expensive.

---

## What's in this repo

| Artifact | What it does |
|---|---|
| [`notebooks/diagnostics.py`](#1-cost--performance-dashboard) | Import into Databricks — SQL dashboard for spend, idle clusters, and full table scans |
| [`notebooks/notebook_scanner.py`](#2-notebook-anti-pattern-scanner) | Import into Databricks — scans notebook code for expensive patterns via widgets |
| [`/diagnose-pipelines` skill](#3-diagnose-pipelines-claude-code-skill) | Claude Code slash command — runs a full health check on all your important pipelines |
| [`server/`](#4-mcp-server-experimental) | FastMCP server exposing the same diagnostics as callable tools (experimental) |

---

## 1. Cost & Performance Dashboard

**File:** `notebooks/diagnostics.py`

A self-contained Databricks notebook. Import it, attach to any cluster, run top to bottom. Each section is independent — run only what you need.

**Sections:**
- Most expensive jobs this week (queries `system.lakeflow.job_run_timeline`)
- Clusters running but idle for >1 hour
- Recent queries with full table scans (no partition pruning, >1M rows read)
- Notebook anti-pattern quick-scan (paste source into the last cell)

**How to use:**
1. In your Databricks workspace: **Workspace → Import → File** → select `notebooks/diagnostics.py`
2. Attach to any running cluster
3. Run All

No credentials needed — runs as your workspace user.

---

## 2. Notebook Anti-Pattern Scanner

**File:** `notebooks/notebook_scanner.py`

A widget-driven notebook that reads notebook source directly from your workspace (no copy-pasting) and flags expensive code patterns.

**Patterns it catches:**

| Pattern | Severity | Why it's bad |
|---|---|---|
| `.collect()` | HIGH | Pulls all data to driver — OOM on large tables |
| `.toPandas()` | HIGH | Same as collect — moves everything to driver memory |
| `repartition(1)` / `coalesce(1)` | MEDIUM | Serializes the job into a single task |
| `ORDER BY` without `LIMIT` | MEDIUM | Full shuffle across all rows |
| Repeated reads without `.cache()` | MEDIUM | Rescans the source on every reference |
| `SELECT *` | LOW | Reads columns you don't need |
| `.show()` with no args | LOW | Triggers full execution for 20 rows |

**How to use:**
1. Import `notebooks/notebook_scanner.py` into your workspace
2. Set the **path** widget to a notebook or folder path (e.g. `/Users/you@company.com/my_project`)
3. Choose **Single notebook** or **Folder** scan mode
4. Optionally set **Save report → Yes** to append findings to a Delta table
5. Run All

Findings are displayed inline. If you save reports, query them later:
```sql
SELECT * FROM main.default.notebook_scan_reports ORDER BY scan_timestamp DESC
```

---

## 3. `/diagnose-pipelines` Claude Code Skill

**Files:** `.claude/commands/diagnose-pipelines.md`, `config/pipelines.yaml`, `scripts/run_diagnostics.py`

A Claude Code slash command that sweeps all your important pipelines in one shot and tells you what's broken. Designed to be run weekly or after major changes.

**Setup:**

```bash
git clone https://github.com/Tanarhw/databricks-workload-optimizer
cd databricks-workload-optimizer
uv sync
cp .env.example .env        # fill in DATABRICKS_HOST and DATABRICKS_TOKEN
```

Edit `config/pipelines.yaml` with your real pipeline IDs:

```yaml
jobs:
  - id: "12345"
    name: "Daily ingestion"

clusters:
  - id: "abc-123-def"
    name: "Production cluster"

tables:
  - name: "main.analytics.events"
```

**Running it:**

```bash
claude          # open Claude Code from the project directory
/diagnose-pipelines
```

Claude runs the diagnostic script, displays a severity-sorted report, and offers to dig deeper into any flagged resource.

**What the report looks like:**
```
============================================================
  Databricks Pipeline Diagnostics — 2026-05-15 14:00 UTC
============================================================

  ✅ [JOB] Daily ingestion (12345) — clean

  [CLUSTER] Production cluster (abc-123-def)
      🔴 HIGH: Fixed 0-worker cluster with no autoscaling.
      🟡 MEDIUM: Photon not enabled.

  [TABLE] main.analytics.events
      🔴 HIGH: OPTIMIZE has never been run on this table.

============================================================
  3 resource(s) checked | 3 issue(s) found | 2 HIGH
```

**Scheduling it weekly:**

Run `/schedule` inside Claude Code and set the prompt to `/diagnose-pipelines`.

**Checks performed:**

| Resource | Checks |
|---|---|
| Jobs | Avg/max duration, failure rate, single-worker clusters |
| Clusters | Photon, autoscaling bounds, spot vs on-demand, AQE config |
| Tables | Avg file size, last OPTIMIZE, Z-ORDER presence, ANALYZE TABLE |

---

## 4. MCP Server (experimental)

A FastMCP + FastAPI server that exposes the same diagnostics as callable tools for Claude or Databricks Agent Bricks. Not required for any of the above — kept for local experimentation.

```bash
cp .env.example .env
uv sync
./scripts/dev/start_server.sh
# MCP endpoint: http://localhost:8000/mcp
```

| Tool | Status |
|---|---|
| `analyze_job(job_id)` | ✅ implemented |
| `check_cluster_config(cluster_id)` | ✅ implemented |
| `scan_table_health(table_name)` | ✅ implemented |
| `explain_query_plan(query)` | 🔜 planned |
