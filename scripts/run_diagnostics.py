#!/usr/bin/env python3
"""Weekly pipeline diagnostic runner. Reads config/pipelines.yaml and checks all resources."""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

try:
    import yaml
except ImportError:
    print("ERROR: pyyaml not installed. Run: uv sync")
    sys.exit(1)

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, StatementState


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def get_client() -> WorkspaceClient:
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    if host and token:
        return WorkspaceClient(host=host, token=token)
    return WorkspaceClient()


def get_warehouse_id(client: WorkspaceClient) -> str | None:
    warehouses = list(client.warehouses.list())
    running = [w for w in warehouses if w.state and w.state.value == "RUNNING"]
    candidates = running or warehouses
    if candidates and candidates[0].id:
        return candidates[0].id
    return None


def run_sql(client: WorkspaceClient, warehouse_id: str, statement: str) -> list[dict]:
    resp = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
        disposition=Disposition.INLINE,
    )
    if resp.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"SQL failed: {resp.status.error}")
    if not resp.result or not resp.result.data_array:
        return []
    columns = [col.name for col in (resp.manifest.schema.columns or [])]
    return [dict(zip(columns, row)) for row in resp.result.data_array]


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def check_job(client: WorkspaceClient, job_id: str, name: str) -> dict:
    findings = []
    try:
        job = client.jobs.get(job_id=int(job_id))
        runs = list(client.jobs.list_runs(job_id=int(job_id), limit=10))
    except Exception as e:
        return {"resource": name, "type": "job", "id": job_id, "error": str(e), "findings": []}

    completed = [r for r in runs if r.state and r.state.result_state]
    durations = [
        (r.end_time - r.start_time) / 60_000
        for r in completed if r.start_time and r.end_time
    ]
    failures = [
        r for r in completed
        if r.state and r.state.result_state
        and r.state.result_state.value in ("FAILED", "TIMEDOUT")
    ]

    if durations:
        avg = sum(durations) / len(durations)
        if avg > 60:
            findings.append({"severity": "HIGH", "message": f"Averaging {avg:.0f} min — over an hour per run."})
        elif avg > 30:
            findings.append({"severity": "MEDIUM", "message": f"Averaging {avg:.0f} min — moderately slow."})

    if completed:
        fail_rate = len(failures) / len(completed) * 100
        if fail_rate >= 50:
            findings.append({"severity": "HIGH", "message": f"{fail_rate:.0f}% failure rate in recent runs."})
        elif fail_rate > 0:
            findings.append({"severity": "MEDIUM", "message": f"{fail_rate:.0f}% failure rate in recent runs."})

    if job.settings and job.settings.job_clusters:
        for jc in job.settings.job_clusters:
            nc = jc.new_cluster
            if nc and not nc.autoscale and nc.num_workers and nc.num_workers < 2:
                findings.append({"severity": "MEDIUM", "message": "Single-worker fixed cluster — no parallelism headroom."})

    return {"resource": name, "type": "job", "id": job_id, "findings": findings}


def check_cluster(client: WorkspaceClient, cluster_id: str, name: str) -> dict:
    findings = []
    try:
        c = client.clusters.get(cluster_id=cluster_id)
    except Exception as e:
        return {"resource": name, "type": "cluster", "id": cluster_id, "error": str(e), "findings": []}

    runtime = c.runtime_engine.value if c.runtime_engine else "STANDARD"
    if runtime != "PHOTON":
        findings.append({"severity": "MEDIUM", "message": "Photon not enabled — highest ROI config change for SQL/Delta workloads."})

    if c.autoscale:
        mx = c.autoscale.max_workers
        if mx and mx < 3:
            findings.append({"severity": "MEDIUM", "message": f"Autoscale max is only {mx} workers — likely to bottleneck."})
    else:
        nw = c.num_workers or 0
        if nw <= 1:
            findings.append({"severity": "HIGH", "message": f"Fixed {nw}-worker cluster with no autoscaling."})
        else:
            findings.append({"severity": "LOW", "message": f"Fixed {nw}-worker cluster — paying for workers even when idle."})

    spot_status = None
    if c.aws_attributes and c.aws_attributes.availability:
        spot_status = c.aws_attributes.availability.value
    elif c.azure_attributes and c.azure_attributes.availability:
        spot_status = c.azure_attributes.availability.value
    if spot_status and "ON_DEMAND" in spot_status:
        findings.append({"severity": "LOW", "message": "All on-demand workers — spot-with-fallback could cut costs ~70%."})

    spark_conf = c.spark_conf or {}
    if spark_conf.get("spark.sql.adaptive.enabled", "").lower() == "false":
        findings.append({"severity": "MEDIUM", "message": "AQE explicitly disabled — remove this override."})

    return {"resource": name, "type": "cluster", "id": cluster_id, "findings": findings}


def check_table(client: WorkspaceClient, warehouse_id: str | None, table_name: str) -> dict:
    findings = []

    if not warehouse_id:
        return {"resource": table_name, "type": "table", "id": table_name,
                "error": "No running SQL warehouse found", "findings": []}

    try:
        detail = run_sql(client, warehouse_id, f"DESCRIBE DETAIL {table_name}")
    except RuntimeError as e:
        return {"resource": table_name, "type": "table", "id": table_name, "error": str(e), "findings": []}

    if detail:
        d = detail[0]
        num_files = int(d.get("numFiles") or 0)
        size_bytes = int(d.get("sizeInBytes") or 0)
        if num_files > 0 and size_bytes > 0:
            avg_mb = (size_bytes / num_files) / (1024 ** 2)
            if avg_mb < 32:
                findings.append({"severity": "HIGH", "message": f"Small file problem: avg {avg_mb:.1f} MB (run OPTIMIZE)."})
            elif avg_mb < 64:
                findings.append({"severity": "MEDIUM", "message": f"Files on the small side: avg {avg_mb:.1f} MB (consider OPTIMIZE)."})

    try:
        history = run_sql(client, warehouse_id, f"DESCRIBE HISTORY {table_name} LIMIT 20")
    except RuntimeError:
        history = []

    ops = {(row.get("operation") or "").upper() for row in history}
    if "OPTIMIZE" not in ops:
        findings.append({"severity": "HIGH", "message": "OPTIMIZE has never been run on this table."})

    has_zorder = any(
        "OPTIMIZE" in (row.get("operation") or "").upper()
        and json.loads(row.get("operationParameters") or "{}").get("zOrderBy", "[]") not in ("[]", [], None)
        for row in history
        if row.get("operationParameters")
    )
    if not has_zorder:
        findings.append({"severity": "MEDIUM", "message": "No Z-ORDER configured — queries can't skip irrelevant files."})

    analyze_ops = {"ANALYZE", "ANALYZE TABLE"}
    if not ops.intersection(analyze_ops):
        findings.append({"severity": "MEDIUM", "message": "ANALYZE TABLE never run — query planner has no statistics."})

    return {"resource": table_name, "type": "table", "id": table_name, "findings": findings}


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

SEVERITY_ORDER = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
SEVERITY_LABEL = {"HIGH": "🔴 HIGH", "MEDIUM": "🟡 MEDIUM", "LOW": "🔵 LOW"}


def print_report(results: list[dict]) -> int:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'='*60}")
    print(f"  Databricks Pipeline Diagnostics — {timestamp}")
    print(f"{'='*60}\n")

    high_count = 0
    for r in results:
        label = f"[{r['type'].upper()}] {r['resource']} ({r['id']})"
        if "error" in r:
            print(f"  ⚠️  {label}")
            print(f"      Error: {r['error']}\n")
            continue

        findings = sorted(r["findings"], key=lambda f: SEVERITY_ORDER.get(f["severity"], 9))
        high_count += sum(1 for f in findings if f["severity"] == "HIGH")

        if not findings:
            print(f"  ✅ {label} — clean\n")
        else:
            print(f"  {label}")
            for f in findings:
                print(f"      {SEVERITY_LABEL[f['severity']]}: {f['message']}")
            print()

    total_issues = sum(len(r.get("findings", [])) for r in results)
    print(f"{'='*60}")
    print(f"  {len(results)} resource(s) checked | {total_issues} issue(s) found | {high_count} HIGH\n")

    return high_count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    config_path = Path(__file__).parent.parent / "config" / "pipelines.yaml"
    if not config_path.exists():
        print(f"No config found at {config_path}")
        print("Edit config/pipelines.yaml with your jobs, clusters, and tables.")
        sys.exit(1)

    with open(config_path) as f:
        config = yaml.safe_load(f) or {}

    client = get_client()
    warehouse_id = get_warehouse_id(client)
    results = []

    for job in config.get("jobs", []):
        print(f"Checking job: {job['name']}...")
        results.append(check_job(client, str(job["id"]), job["name"]))

    for cluster in config.get("clusters", []):
        print(f"Checking cluster: {cluster['name']}...")
        results.append(check_cluster(client, cluster["id"], cluster["name"]))

    for table in config.get("tables", []):
        print(f"Checking table: {table['name']}...")
        results.append(check_table(client, warehouse_id, table["name"]))

    high_count = print_report(results)
    sys.exit(1 if high_count > 0 else 0)


if __name__ == "__main__":
    main()
