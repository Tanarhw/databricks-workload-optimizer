# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Anti-Pattern Scanner
# MAGIC Scans one notebook or an entire folder for expensive code patterns.
# MAGIC Configure with the widgets above, then **Run All**.

# COMMAND ----------

# Widget setup — run this cell first if widgets aren't showing
dbutils.widgets.removeAll()

dbutils.widgets.dropdown(
    "scan_mode", "Single notebook",
    ["Single notebook", "Folder (all notebooks)"],
    "Scan mode",
)
dbutils.widgets.text(
    "path", "",
    "Notebook or folder path",
)
dbutils.widgets.dropdown(
    "save_report", "No", ["No", "Yes"],
    "Save report to Delta table",
)
dbutils.widgets.text(
    "report_table", "main.default.notebook_scan_reports",
    "Report table (if saving)",
)

# COMMAND ----------
# MAGIC %md ## Step 1 — Load notebooks from workspace

# COMMAND ----------

import base64
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ObjectType

scan_mode = dbutils.widgets.get("scan_mode")
path = dbutils.widgets.get("path").strip()

if not path:
    # Default to current user's home directory
    current_user = spark.sql("SELECT current_user()").collect()[0][0]
    path = f"/Users/{current_user}"
    print(f"No path set — defaulting to {path}")

w = WorkspaceClient()


def read_notebook_source(notebook_path: str) -> str:
    result = w.workspace.export(notebook_path, format=ExportFormat.SOURCE)
    return base64.b64decode(result.content).decode("utf-8")


notebooks_to_scan: list[tuple[str, str]] = []

if scan_mode == "Single notebook":
    try:
        source = read_notebook_source(path)
        notebooks_to_scan = [(path, source)]
    except Exception as e:
        raise RuntimeError(f"Could not read notebook at '{path}': {e}")
else:
    try:
        items = list(w.workspace.list(path, recursive=False))
    except Exception as e:
        raise RuntimeError(f"Could not list folder '{path}': {e}")

    for item in items:
        if item.object_type == ObjectType.NOTEBOOK and item.path:
            try:
                source = read_notebook_source(item.path)
                notebooks_to_scan.append((item.path, source))
            except Exception as e:
                print(f"  Skipping {item.path}: {e}")

print(f"Loaded {len(notebooks_to_scan)} notebook(s) from '{path}'")

# COMMAND ----------
# MAGIC %md ## Step 2 — Scan for anti-patterns

# COMMAND ----------

import re
from dataclasses import dataclass, field, asdict


@dataclass
class Issue:
    notebook: str
    line: str
    severity: str
    pattern: str
    detail: str
    fix: str
    code: str = ""


PATTERNS = [
    {
        "name": ".collect() without limit",
        "regex": r"\.collect\(\)",
        "severity": "HIGH",
        "detail": "Pulls the entire DataFrame to the driver. Will OOM on anything >1 GB.",
        "fix": "Aggregate first, or use .limit(n).collect() for samples.",
    },
    {
        "name": ".toPandas() on large DataFrame",
        "regex": r"\.toPandas\(\)",
        "severity": "HIGH",
        "detail": "Same as .collect() — moves all data to driver memory.",
        "fix": "Sample first: .limit(10000).toPandas(), or use Pandas API on Spark.",
    },
    {
        "name": "repartition or coalesce to 1",
        "regex": r"\.(repartition|coalesce)\(\s*1\s*\)",
        "severity": "MEDIUM",
        "detail": "Forces a full shuffle into a single task — serializes the job.",
        "fix": "Only use this immediately before writing a single-file output. Remove from intermediate steps.",
    },
    {
        "name": ".show() with no limit",
        "regex": r"\.show\(\s*\)",
        "severity": "LOW",
        "detail": "Triggers full DataFrame execution even though it only displays 20 rows.",
        "fix": "Fine for notebooks; remove from production pipeline code.",
    },
    {
        "name": "ORDER BY without LIMIT on large table",
        "regex": r"(?i)order\s+by\b(?!.*\blimit\b)",
        "severity": "MEDIUM",
        "detail": "Sorting without LIMIT requires a full shuffle across all rows.",
        "fix": "Add LIMIT, or sort after aggregating to a smaller result set.",
    },
    {
        "name": "SELECT * (no column pruning)",
        "regex": r"(?i)select\s+\*\s+from",
        "severity": "LOW",
        "detail": "Reads all columns including ones you don't need. Hurts Parquet/Delta performance.",
        "fix": "Select only the columns you need.",
    },
    {
        "name": "print() on DataFrame",
        "regex": r"\bprint\s*\(\s*\w*[Dd][Ff]\w*\s*\)",
        "severity": "LOW",
        "detail": "Prints the DataFrame object, not its contents. Likely a debug artifact.",
        "fix": "Use display(df) or df.show() if you need to inspect the data.",
    },
]


def scan_source(notebook_path: str, source: str) -> list[Issue]:
    issues: list[Issue] = []
    lines = source.splitlines()

    for i, line in enumerate(lines, 1):
        stripped = line.strip()
        if stripped.startswith("#") or stripped.startswith("//"):
            continue

        for p in PATTERNS:
            if re.search(p["regex"], stripped):
                issues.append(Issue(
                    notebook=notebook_path,
                    line=str(i),
                    severity=p["severity"],
                    pattern=p["name"],
                    detail=p["detail"],
                    fix=p["fix"],
                    code=stripped[:120],
                ))

    # Multi-line: repeated reads without cache
    reads = re.findall(
        r'spark\.read[^)]*\.(?:table|load|parquet|csv|json|orc)\(["\']([^"\']+)["\']',
        source,
    )
    seen: dict[str, int] = {}
    for table in reads:
        seen[table] = seen.get(table, 0) + 1
    for table, count in seen.items():
        if count > 1:
            issues.append(Issue(
                notebook=notebook_path,
                line="—",
                severity="MEDIUM",
                pattern="Repeated read without cache",
                detail=f"'{table}' is read {count} times. Each read rescans the full source.",
                fix=f"Read once and cache: df = spark.read...table('{table}'); df.cache()",
            ))

    return issues


all_issues: list[Issue] = []
for nb_path, nb_source in notebooks_to_scan:
    found = scan_source(nb_path, nb_source)
    all_issues.extend(found)
    status = f"{len(found)} issue(s)" if found else "clean"
    print(f"  {nb_path.split('/')[-1]}: {status}")

print(f"\nTotal: {len(all_issues)} issue(s) across {len(notebooks_to_scan)} notebook(s)")

# COMMAND ----------
# MAGIC %md ## Step 3 — Results

# COMMAND ----------

if not all_issues:
    print("No issues found.")
else:
    import pandas as pd
    from IPython.display import display as ipy_display

    df = pd.DataFrame([asdict(i) for i in all_issues])
    df = df.sort_values(["severity", "notebook", "line"], key=lambda col: col.map(
        {"HIGH": 0, "MEDIUM": 1, "LOW": 2}) if col.name == "severity" else col
    )
    display(spark.createDataFrame(df))

# COMMAND ----------
# MAGIC %md ## Step 4 — Save report (optional)

# COMMAND ----------

from datetime import datetime, timezone

save_report = dbutils.widgets.get("save_report")
report_table = dbutils.widgets.get("report_table").strip()

if save_report == "Yes":
    if not all_issues:
        print("Nothing to save — no issues found.")
    else:
        import pandas as pd
        from pyspark.sql.functions import lit

        report_df = spark.createDataFrame(
            pd.DataFrame([asdict(i) for i in all_issues])
        ).withColumn("scan_timestamp", lit(datetime.now(timezone.utc).isoformat())) \
         .withColumn("scanned_path", lit(path))

        report_df.write.format("delta").mode("append").saveAsTable(report_table)
        print(f"Report appended to {report_table} ({len(all_issues)} rows)")
        print(f"Query it: SELECT * FROM {report_table} ORDER BY scan_timestamp DESC")
else:
    print("Report not saved. Set 'Save report to Delta table' widget to 'Yes' to persist findings.")
