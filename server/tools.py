import json

from fastmcp import FastMCP

from server.utils import get_client, get_warehouse_id, run_sql


def register_tools(mcp: FastMCP) -> None:
    @mcp.tool()
    def analyze_job(job_id: str) -> str:
        """Analyze a Databricks job for performance issues and return a plain-English diagnosis."""
        client = get_client()

        try:
            job = client.jobs.get(job_id=int(job_id))
        except Exception as e:
            return f"Could not fetch job {job_id}: {e}"

        runs = list(client.jobs.list_runs(job_id=int(job_id), limit=10))
        if not runs:
            return f"Job '{job.settings.name}' exists but has no run history."

        completed = [r for r in runs if r.state and r.state.result_state]
        durations_min: list[float] = []
        failed_runs = []

        for run in completed:
            if run.start_time and run.end_time:
                durations_min.append((run.end_time - run.start_time) / 60_000)
            if run.state and run.state.result_state:
                if run.state.result_state.value in ("FAILED", "TIMEDOUT"):
                    failed_runs.append(run)

        lines = [f"## Job: {job.settings.name} (ID: {job_id})\n"]

        if durations_min:
            avg = sum(durations_min) / len(durations_min)
            longest = max(durations_min)
            lines += [
                f"**Recent performance ({len(durations_min)} completed runs):**",
                f"- Average duration: {avg:.1f} min",
                f"- Longest run: {longest:.1f} min",
            ]

            if avg > 60:
                lines += [
                    "\n**Diagnosis: Averaging over an hour — this job is expensive.**",
                    "Likely causes:",
                    "- Full table scans with no partition pruning or Z-ordering",
                    "- Cluster undersized for data volume",
                    "- Missing OPTIMIZE/VACUUM on Delta tables being read",
                    "\nFixes to try:",
                    "- Add Z-ORDER on high-cardinality filter columns",
                    "- Enable Photon on the cluster",
                    "- Add `spark.databricks.delta.optimizeWrite.enabled true` to cluster config",
                ]
            elif avg > 30:
                lines += [
                    "\n**Diagnosis: Moderately slow (30–60 min range).**",
                    "Common causes:",
                    "- Shuffle-heavy joins — try broadcast hints for small tables",
                    "- Autoscaling not configured or max workers too low",
                ]
            else:
                lines.append("\n**Diagnosis: Job duration looks healthy.**")

        if failed_runs:
            rate = len(failed_runs) / len(completed) * 100
            lines += [
                f"\n**Reliability: {rate:.0f}% failure rate in recent runs**",
                "Common causes:",
                "- OOM from undersized cluster or skewed data",
                "- Timeout from excessive duration",
                "- Source table schema changes or missing files",
            ]
        elif completed:
            lines.append("\n**Reliability: No failures in recent runs.**")

        # Flag single-worker fixed clusters
        if job.settings.job_clusters:
            for jc in job.settings.job_clusters:
                nc = jc.new_cluster
                if nc and not nc.autoscale and nc.num_workers and nc.num_workers < 2:
                    lines += [
                        "\n**Cluster warning:** Single-worker cluster detected.",
                        "For jobs processing >1 GB, use autoscaling or at least 2 workers.",
                    ]

        return "\n".join(lines)

    @mcp.tool()
    def check_cluster_config(cluster_id: str) -> str:
        """Inspect a cluster's configuration and flag settings that are wrong for the workload type."""
        client = get_client()

        try:
            c = client.clusters.get(cluster_id=cluster_id)
        except Exception as e:
            return f"Could not fetch cluster {cluster_id}: {e}"

        lines = [f"## Cluster: {c.cluster_name} (ID: {cluster_id})\n"]
        warnings: list[str] = []
        good: list[str] = []

        # --- Photon ---
        runtime = (c.runtime_engine.value if c.runtime_engine else "STANDARD")
        if runtime == "PHOTON":
            good.append("Photon runtime enabled — vectorized execution active.")
        else:
            warnings.append(
                "**Photon not enabled.** For SQL/Delta workloads this is the single highest-ROI change.\n"
                "  Fix: Edit cluster → Runtime → check 'Use Photon Acceleration'."
            )

        # --- Autoscaling ---
        if c.autoscale:
            mn, mx = c.autoscale.min_workers, c.autoscale.max_workers
            good.append(f"Autoscaling configured ({mn}–{mx} workers).")
            if mx and mx < 3:
                warnings.append(
                    f"**Autoscale max is only {mx} workers.** For non-trivial jobs this will bottleneck.\n"
                    "  Fix: Raise max_workers to at least 4–8 depending on data volume."
                )
        else:
            nw = c.num_workers or 0
            if nw == 0:
                warnings.append(
                    "**Single-node cluster (driver only, no workers).** Fine for development; "
                    "not suitable for production jobs processing >1 GB.\n"
                    "  Fix: Enable autoscaling or set num_workers ≥ 2."
                )
            elif nw == 1:
                warnings.append(
                    "**Fixed 1-worker cluster.** No parallelism headroom — any spike will queue.\n"
                    "  Fix: Switch to autoscaling (min 1, max 4+)."
                )
            else:
                warnings.append(
                    f"**Fixed {nw}-worker cluster (no autoscaling).** You're paying for {nw} workers even when idle.\n"
                    "  Fix: Enable autoscaling to scale down during quiet periods."
                )

        # --- Spot vs on-demand ---
        spot_status = None
        if c.aws_attributes:
            spot_status = c.aws_attributes.availability.value if c.aws_attributes.availability else None
        elif c.azure_attributes:
            spot_status = c.azure_attributes.availability.value if c.azure_attributes.availability else None
        elif c.gcp_attributes:
            spot_status = c.gcp_attributes.availability.value if c.gcp_attributes.availability else None

        if spot_status:
            if "ON_DEMAND" in spot_status:
                warnings.append(
                    "**All on-demand instances.** Workers can safely run on spot/preemptible to cut costs ~70%.\n"
                    "  Fix: Set workers to SPOT_WITH_FALLBACK — driver should stay on-demand."
                )
            elif "SPOT_WITH_FALLBACK" in spot_status:
                good.append("Spot-with-fallback configured — good cost/reliability balance.")
            elif "SPOT" in spot_status:
                good.append("Spot instances in use — cost-efficient.")

        # --- Node type heuristics ---
        node_type = c.node_type_id or ""
        if node_type:
            lines.append(f"**Node type:** `{node_type}`")
            # AWS memory-optimized: r5/r6, Azure: Standard_E, GCP: n2-highmem
            is_memory = any(x in node_type for x in ("r5", "r6", "r7", "Standard_E", "highmem"))
            is_compute = any(x in node_type for x in ("c5", "c6", "Standard_F", "highcpu"))
            if is_memory:
                good.append(f"Memory-optimized instance (`{node_type}`) — good for large joins and aggregations.")
            elif is_compute:
                warnings.append(
                    f"**Compute-optimized instance (`{node_type}`).** These are CPU-heavy but low on RAM.\n"
                    "  If your jobs do large joins or handle wide DataFrames, switch to a memory-optimized type."
                )

        # --- Key Spark configs ---
        spark_conf = c.spark_conf or {}

        aqe = spark_conf.get("spark.sql.adaptive.enabled", "").lower()
        if aqe == "false":
            warnings.append(
                "**Adaptive Query Execution (AQE) is explicitly disabled.**\n"
                "  AQE auto-tunes shuffle partitions and handles skew — almost always beneficial.\n"
                "  Fix: Remove this override or set `spark.sql.adaptive.enabled` to `true`."
            )
        else:
            good.append("AQE enabled (default on Databricks Runtime 7+).")

        shuffle_partitions = spark_conf.get("spark.sql.shuffle.partitions")
        if shuffle_partitions:
            val = int(shuffle_partitions)
            if val < 8:
                warnings.append(
                    f"**`spark.sql.shuffle.partitions` is set very low ({val}).**\n"
                    "  This will serialize shuffles on larger datasets.\n"
                    "  Fix: Remove the override and let AQE tune it automatically."
                )
            elif val > 2000:
                warnings.append(
                    f"**`spark.sql.shuffle.partitions` is set very high ({val}).**\n"
                    "  Excessive partitions add scheduling overhead.\n"
                    "  Fix: Remove the override and let AQE tune it automatically."
                )

        # --- Assemble output ---
        if warnings:
            lines.append("### Issues found\n")
            for w in warnings:
                lines.append(f"- {w}\n")
        else:
            lines.append("No major configuration issues found.\n")

        if good:
            lines.append("### Looks good\n")
            for g in good:
                lines.append(f"- {g}")

        return "\n".join(lines)

    @mcp.tool()
    def scan_table_health(table_name: str) -> str:
        """Check a Delta table for small files, missing Z-order, and stale statistics."""
        client = get_client()

        try:
            warehouse_id = get_warehouse_id(client)
        except RuntimeError as e:
            return f"Cannot run table scan — no SQL warehouse available: {e}"

        try:
            detail_rows = run_sql(client, warehouse_id, f"DESCRIBE DETAIL {table_name}")
        except RuntimeError as e:
            return f"Could not describe table '{table_name}': {e}"

        if not detail_rows:
            return f"No detail returned for table '{table_name}' — does it exist?"

        d = detail_rows[0]
        num_files = int(d.get("numFiles") or 0)
        size_bytes = int(d.get("sizeInBytes") or 0)
        table_format = d.get("format", "unknown")
        partition_cols = d.get("partitionColumns") or "[]"

        lines = [f"## Table: {table_name}\n"]
        lines.append(f"Format: {table_format} | Partitioned by: {partition_cols}")

        warnings: list[str] = []
        good: list[str] = []

        # --- File size health ---
        if num_files > 0 and size_bytes > 0:
            avg_mb = (size_bytes / num_files) / (1024 ** 2)
            total_gb = size_bytes / (1024 ** 3)
            lines.append(f"Files: {num_files} | Avg size: {avg_mb:.1f} MB | Total: {total_gb:.2f} GB\n")

            if avg_mb < 32:
                warnings.append(
                    f"**Small file problem: avg file is only {avg_mb:.1f} MB.**\n"
                    "  Spark opens one task per file — thousands of tiny files kill parallelism.\n"
                    "  Fix: `OPTIMIZE {table_name}` — this compacts files to ~128 MB each."
                )
            elif avg_mb < 64:
                warnings.append(
                    f"**Files are on the small side ({avg_mb:.1f} MB avg, target ~128 MB).**\n"
                    "  Fix: Run `OPTIMIZE {table_name}` to compact."
                )
            else:
                good.append(f"File sizes look healthy ({avg_mb:.1f} MB avg).")
        else:
            lines.append("Table appears to be empty.\n")

        # --- History analysis ---
        try:
            history = run_sql(client, warehouse_id, f"DESCRIBE HISTORY {table_name} LIMIT 30")
        except RuntimeError:
            history = []

        last_optimize_ts = None
        last_zorder_cols: list[str] = []
        last_analyze_ts = None

        for row in history:
            op = (row.get("operation") or "").upper()
            ts = row.get("timestamp")

            if op == "OPTIMIZE" and last_optimize_ts is None:
                last_optimize_ts = ts
                params_raw = row.get("operationParameters") or "{}"
                try:
                    params = json.loads(params_raw) if isinstance(params_raw, str) else params_raw
                    zorder_raw = params.get("zOrderBy", "[]")
                    last_zorder_cols = json.loads(zorder_raw) if isinstance(zorder_raw, str) else zorder_raw
                except (json.JSONDecodeError, TypeError):
                    pass

            if op in ("ANALYZE", "ANALYZE TABLE") and last_analyze_ts is None:
                last_analyze_ts = ts

        if last_optimize_ts:
            good.append(f"Last OPTIMIZE ran at {last_optimize_ts}.")
        else:
            warnings.append(
                "**No OPTIMIZE has ever been run on this table.**\n"
                f"  Fix: `OPTIMIZE {table_name}` — compacts small files and improves read performance."
            )

        if last_zorder_cols:
            good.append(f"Z-ORDER active on: {', '.join(last_zorder_cols)}.")
        else:
            warnings.append(
                "**No Z-ORDER configured.**\n"
                "  Z-ORDER co-locates related data so queries skip irrelevant files.\n"
                f"  Fix: `OPTIMIZE {table_name} ZORDER BY (your_filter_column)` — use the column you filter on most."
            )

        if last_analyze_ts:
            good.append(f"Table statistics last updated at {last_analyze_ts}.")
        else:
            warnings.append(
                "**No ANALYZE TABLE has been run — statistics are stale or missing.**\n"
                "  Without statistics, the query planner makes poor join and filter decisions.\n"
                f"  Fix: `ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS`"
            )

        # --- Assemble ---
        if warnings:
            lines.append("### Issues found\n")
            for w in warnings:
                lines.append(f"- {w}\n")
        else:
            lines.append("Table health looks good.\n")

        if good:
            lines.append("### Looks good\n")
            for g in good:
                lines.append(f"- {g}")

        return "\n".join(lines)

    @mcp.tool()
    def explain_query_plan(query: str) -> str:
        """Run EXPLAIN EXTENDED on a query and interpret the Spark plan in plain English."""
        # TODO: implement
        # Plan: execute EXPLAIN EXTENDED via client.statement_execution
        # Parse: identify full scans, large shuffles, skew, missing predicate pushdown
        return f"explain_query_plan not yet implemented (query={query!r})"
