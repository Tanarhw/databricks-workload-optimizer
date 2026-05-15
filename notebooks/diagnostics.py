# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Workload Diagnostics
# MAGIC Personal toolkit — run any section independently. No setup required.

# COMMAND ----------
# MAGIC %md ## 1. Most expensive jobs this week

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   job_name,
# MAGIC   COUNT(*) AS runs,
# MAGIC   ROUND(AVG((end_time - start_time) / 60000), 1) AS avg_duration_min,
# MAGIC   ROUND(MAX((end_time - start_time) / 60000), 1) AS max_duration_min,
# MAGIC   SUM(total_dbu) AS total_dbus
# MAGIC FROM system.lakeflow.job_run_timeline
# MAGIC WHERE start_time >= DATEADD(day, -7, current_timestamp())
# MAGIC GROUP BY job_name
# MAGIC ORDER BY total_dbus DESC
# MAGIC LIMIT 20

# COMMAND ----------
# MAGIC %md ## 2. Clusters running but idle

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   cluster_name,
# MAGIC   cluster_id,
# MAGIC   state,
# MAGIC   ROUND((unix_timestamp() - last_activity_time / 1000) / 3600, 1) AS idle_hours
# MAGIC FROM system.compute.clusters
# MAGIC WHERE state = 'RUNNING'
# MAGIC   AND last_activity_time IS NOT NULL
# MAGIC   AND (unix_timestamp() - last_activity_time / 1000) > 3600
# MAGIC ORDER BY idle_hours DESC

# COMMAND ----------
# MAGIC %md ## 3. Recent queries with full table scans

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   statement_id,
# MAGIC   statement_text,
# MAGIC   total_task_duration_ms,
# MAGIC   read_rows,
# MAGIC   produced_rows
# MAGIC FROM system.query.history
# MAGIC WHERE executed_by = current_user()
# MAGIC   AND start_time >= DATEADD(day, -7, current_timestamp())
# MAGIC   AND read_partitions = 0
# MAGIC   AND read_rows > 1000000
# MAGIC ORDER BY read_rows DESC
# MAGIC LIMIT 20

# COMMAND ----------
# MAGIC %md ## 4. Notebook code anti-pattern scanner
# MAGIC Paste your notebook source below and run the cell.

# COMMAND ----------

NOTEBOOK_SOURCE = """
# paste your notebook code here
"""

# COMMAND ----------

import re

def scan_notebook(source: str) -> list[dict]:
    issues = []
    lines = source.splitlines()

    for i, line in enumerate(lines, 1):
        stripped = line.strip()

        if re.search(r'\.collect\(\)', stripped) and not stripped.startswith('#'):
            issues.append({
                "line": i,
                "severity": "HIGH",
                "issue": ".collect() on a DataFrame",
                "detail": "Pulls all data to the driver — will OOM on large tables.",
                "fix": "Use .limit(n).collect() for samples, or aggregate first with .groupBy().agg().",
                "code": stripped,
            })

        if re.search(r'\.toPandas\(\)', stripped) and not stripped.startswith('#'):
            issues.append({
                "line": i,
                "severity": "HIGH",
                "issue": ".toPandas() on a DataFrame",
                "detail": "Same problem as .collect() — moves everything to driver memory.",
                "fix": "Sample first: .limit(10000).toPandas(), or use Pandas on Spark (spark.createDataFrame).",
                "code": stripped,
            })

        if re.search(r'spark\.sql\(|\.load\(|spark\.read', stripped) and not stripped.startswith('#'):
            if re.search(r'\.cache\(\)|\.persist\(', source[source.find(stripped):source.find(stripped) + 500]):
                pass
            else:
                # Only flag if same table/query appears more than once
                pass  # full multi-pass check below

        if re.search(r'\.repartition\(\s*1\s*\)|\.coalesce\(\s*1\s*\)', stripped) and not stripped.startswith('#'):
            issues.append({
                "line": i,
                "severity": "MEDIUM",
                "issue": "Repartitioning to 1 partition",
                "detail": "Forces all data through a single task — serializes your job.",
                "fix": "Only use coalesce(1) before writing a single output file. Remove it from intermediate steps.",
                "code": stripped,
            })

        if re.search(r'\.show\(\s*\)', stripped) and not stripped.startswith('#'):
            issues.append({
                "line": i,
                "severity": "LOW",
                "issue": ".show() with no limit",
                "detail": "Triggers a full DataFrame computation even though it only displays 20 rows.",
                "fix": "Fine for debugging; remove from production pipelines.",
                "code": stripped,
            })

    # Repeated read detection
    reads = re.findall(r'spark\.read[^)]+\.(?:table|load|parquet|csv|json)\(["\']([^"\']+)["\']', source)
    seen = {}
    for table in reads:
        seen[table] = seen.get(table, 0) + 1
    for table, count in seen.items():
        if count > 1:
            issues.append({
                "line": "—",
                "severity": "MEDIUM",
                "issue": f"'{table}' read {count} times without caching",
                "detail": "Each read re-scans the source. On large tables this multiplies your cost.",
                "fix": f"Read once, cache: df = spark.read...table('{table}'); df.cache()",
                "code": "",
            })

    return issues


results = scan_notebook(NOTEBOOK_SOURCE)

if not results:
    print("No issues found.")
else:
    for r in results:
        print(f"[{r['severity']}] Line {r['line']}: {r['issue']}")
        print(f"  Why:  {r['detail']}")
        print(f"  Fix:  {r['fix']}")
        if r['code']:
            print(f"  Code: {r['code']}")
        print()
