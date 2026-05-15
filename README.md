# Databricks Workload Optimizer

Personal diagnostic toolkit for Databricks — surfaces slow jobs, expensive queries, and table health issues without requiring any server setup or MCP configuration.

## What this is

Two tools in one repo:

### 1. Diagnostic Notebook (primary)
A self-contained Databricks notebook you import directly into your workspace and run. No server, no auth config, no MCP setup — just paste and execute.

**What it checks:**
- Your most expensive jobs this week (system tables)
- Idle or oversized clusters burning DBUs
- Notebook code anti-patterns: `collect()` on large DataFrames, missing `cache()`, repeated queries, full table scans
- Table health: small files, missing OPTIMIZE/ZORDER, stale statistics

→ See [`notebooks/diagnostics.py`](notebooks/diagnostics.py)

### 2. MCP Server (secondary)
A FastMCP + FastAPI server exposing the same diagnostics as callable tools for Claude or Databricks Agent Bricks. Intended for future use or local experimentation — not required for the notebook.

→ See [`server/`](server/)

## Using the diagnostic notebook

1. Download [`notebooks/diagnostics.py`](notebooks/diagnostics.py)
2. In your Databricks workspace: **Import** → select the file
3. Attach to any running cluster
4. Run cells top to bottom — each section is independent

No credentials needed — runs as your workspace user via the notebook's ambient auth.

## Running the MCP server locally

```bash
cp .env.example .env   # fill in DATABRICKS_HOST and DATABRICKS_TOKEN
uv sync
./scripts/dev/start_server.sh
# MCP endpoint: http://localhost:8000/mcp
```

## MCP tools

| Tool | Status |
|---|---|
| `analyze_job(job_id)` | ✅ implemented |
| `check_cluster_config(cluster_id)` | ✅ implemented |
| `scan_table_health(table_name)` | ✅ implemented |
| `explain_query_plan(query)` | 🔜 planned |
