#!/usr/bin/env python3
"""Weekly pipeline diagnostic runner. Reads config/pipelines.yaml and checks all resources."""

import base64
import json
import os
import re
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
from databricks.sdk.service.workspace import ExportFormat

REPORTS_DIR = Path(__file__).parent.parent / "reports"
LAST_RUN_PATH = REPORTS_DIR / "last_run.json"


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


NOTEBOOK_PATTERNS = [
    {"name": ".collect() without limit", "regex": r"\.collect\(\)", "severity": "HIGH",
     "detail": "Pulls entire DataFrame to driver — OOM on large tables.",
     "fix": "Aggregate first, or use .limit(n).collect() for samples."},
    {"name": ".toPandas() on large DataFrame", "regex": r"\.toPandas\(\)", "severity": "HIGH",
     "detail": "Same as .collect() — moves all data to driver memory.",
     "fix": "Sample first: .limit(10000).toPandas()"},
    {"name": "repartition or coalesce to 1", "regex": r"\.(repartition|coalesce)\(\s*1\s*\)", "severity": "MEDIUM",
     "detail": "Forces a full shuffle into a single task — serializes the job.",
     "fix": "Only use before writing a single-file output."},
    {"name": "ORDER BY without LIMIT", "regex": r"(?i)order\s+by\b(?!.*\blimit\b)", "severity": "MEDIUM",
     "detail": "Full shuffle across all rows.",
     "fix": "Add LIMIT, or sort after aggregating to a smaller result set."},
    {"name": "SELECT *", "regex": r"(?i)select\s+\*\s+from", "severity": "LOW",
     "detail": "Reads all columns — hurts Parquet/Delta performance.",
     "fix": "Select only the columns you need."},
]


def check_notebook(client: WorkspaceClient, path: str, name: str) -> dict:
    findings = []
    try:
        result = client.workspace.export(path, format=ExportFormat.SOURCE)
        source = base64.b64decode(result.content).decode("utf-8")
    except Exception as e:
        return {"resource": name, "type": "notebook", "id": path, "error": str(e), "findings": []}

    lines = source.splitlines()
    for i, line in enumerate(lines, 1):
        stripped = line.strip()
        if stripped.startswith("#") or stripped.startswith("//"):
            continue
        for p in NOTEBOOK_PATTERNS:
            if re.search(p["regex"], stripped):
                findings.append({
                    "severity": p["severity"],
                    "message": f"Line {i}: {p['name']} — {p['detail']} Fix: {p['fix']}",
                })

    reads = re.findall(
        r'spark\.read[^)]*\.(?:table|load|parquet|csv|json|orc)\(["\']([^"\']+)["\']', source
    )
    seen: dict[str, int] = {}
    for table in reads:
        seen[table] = seen.get(table, 0) + 1
    for table, count in seen.items():
        if count > 1:
            findings.append({
                "severity": "MEDIUM",
                "message": f"'{table}' read {count}x without caching. Fix: read once and call .cache()",
            })

    return {"resource": name, "type": "notebook", "id": path, "findings": findings}


# ---------------------------------------------------------------------------
# Health score
# ---------------------------------------------------------------------------

SCORE_DEDUCTIONS = {"HIGH": 20, "MEDIUM": 10, "LOW": 5}
GRADE_THRESHOLDS = [(90, "A"), (75, "B"), (60, "C"), (40, "D")]


def calculate_score(results: list[dict]) -> tuple[int, str]:
    score = 100
    for r in results:
        for f in r.get("findings", []):
            score -= SCORE_DEDUCTIONS.get(f["severity"], 0)
    score = max(0, score)
    grade = next((g for threshold, g in GRADE_THRESHOLDS if score >= threshold), "F")
    return score, grade


# ---------------------------------------------------------------------------
# Trend — persist last run locally
# ---------------------------------------------------------------------------

def load_previous_run() -> dict | None:
    if not LAST_RUN_PATH.exists():
        return None
    try:
        with open(LAST_RUN_PATH) as f:
            return json.load(f)
    except Exception:
        return None


def save_current_run(results: list[dict], score: int, grade: str) -> None:
    REPORTS_DIR.mkdir(exist_ok=True)
    snapshot = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "score": score,
        "grade": grade,
        "results": [
            {"resource": r["resource"], "type": r["type"], "id": r["id"],
             "finding_keys": sorted(f["message"][:60] for f in r.get("findings", []))}
            for r in results
        ],
    }
    with open(LAST_RUN_PATH, "w") as f:
        json.dump(snapshot, f, indent=2)


def build_trend(results: list[dict], previous: dict | None) -> dict:
    """Returns per-resource sets of new and resolved issue keys."""
    if not previous:
        return {}

    prev_by_id = {r["id"]: set(r.get("finding_keys", [])) for r in previous.get("results", [])}
    trend = {}
    for r in results:
        rid = r["id"]
        current_keys = {f["message"][:60] for f in r.get("findings", [])}
        prev_keys = prev_by_id.get(rid, set())
        new = current_keys - prev_keys
        resolved = prev_keys - current_keys
        if new or resolved:
            trend[rid] = {"new": new, "resolved": resolved}
    return trend


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

SEVERITY_ORDER = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
SEVERITY_LABEL = {"HIGH": "🔴 HIGH", "MEDIUM": "🟡 MEDIUM", "LOW": "🔵 LOW"}


def print_report(results: list[dict], score: int, grade: str, previous: dict | None, trend: dict) -> int:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Score line with trend
    prev_score = previous.get("score") if previous else None
    prev_grade = previous.get("grade") if previous else None
    if prev_score is not None:
        delta = score - prev_score
        arrow = f"▲ +{delta}" if delta > 0 else (f"▼ {delta}" if delta < 0 else "→ no change")
        score_line = f"  Health: {grade} ({score}/100)  {arrow} vs last run ({prev_grade}, {prev_score}/100)"
    else:
        score_line = f"  Health: {grade} ({score}/100)  (first run — no trend data yet)"

    print(f"\n{'='*62}")
    print(f"  Databricks Pipeline Diagnostics — {timestamp}")
    print(score_line)
    print(f"{'='*62}\n")

    high_count = 0
    for r in results:
        label = f"[{r['type'].upper()}] {r['resource']}"
        resource_trend = trend.get(r["id"], {})

        if "error" in r:
            print(f"  ⚠️  {label}")
            print(f"      Error: {r['error']}\n")
            continue

        findings = sorted(r.get("findings", []), key=lambda f: SEVERITY_ORDER.get(f["severity"], 9))
        high_count += sum(1 for f in findings if f["severity"] == "HIGH")

        if not findings:
            print(f"  ✅ {label} — clean")
            if resource_trend.get("resolved"):
                print(f"      ✨ {len(resource_trend['resolved'])} issue(s) resolved since last run")
            print()
        else:
            print(f"  {label}")
            for f in findings:
                key = f["message"][:60]
                tag = " 🆕" if key in resource_trend.get("new", set()) else ""
                print(f"      {SEVERITY_LABEL[f['severity']]}{tag}: {f['message']}")
            if resource_trend.get("resolved"):
                print(f"      ✨ {len(resource_trend['resolved'])} issue(s) resolved since last run")
            print()

    total_issues = sum(len(r.get("findings", [])) for r in results)
    print(f"{'='*62}")
    print(f"  {len(results)} resource(s) checked | {total_issues} issue(s) | {high_count} HIGH\n")

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
    previous = load_previous_run()
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

    for notebook in config.get("notebooks", []):
        print(f"Scanning notebook: {notebook['name']}...")
        results.append(check_notebook(client, notebook["path"], notebook["name"]))

    score, grade = calculate_score(results)
    trend = build_trend(results, previous)
    high_count = print_report(results, score, grade, previous, trend)
    save_current_run(results, score, grade)

    sys.exit(1 if high_count > 0 else 0)


if __name__ == "__main__":
    main()
