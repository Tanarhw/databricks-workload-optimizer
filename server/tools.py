from fastmcp import FastMCP

from server.utils import get_client


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
        # TODO: implement
        # Plan: fetch cluster via client.clusters.get(cluster_id)
        # Check: autoscaling bounds, instance type family, Photon enabled,
        #        spot vs on-demand mix, relevant Spark configs
        return f"check_cluster_config not yet implemented (cluster_id={cluster_id})"

    @mcp.tool()
    def scan_table_health(table_name: str) -> str:
        """Check a Delta table for small files, missing Z-order, and stale statistics."""
        # TODO: implement
        # Plan: DESCRIBE DETAIL <table>, DESCRIBE HISTORY <table>
        # Check: avg file size, last OPTIMIZE/ZORDER run, last ANALYZE run,
        #        num files vs ideal target
        return f"scan_table_health not yet implemented (table_name={table_name})"

    @mcp.tool()
    def explain_query_plan(query: str) -> str:
        """Run EXPLAIN EXTENDED on a query and interpret the Spark plan in plain English."""
        # TODO: implement
        # Plan: execute EXPLAIN EXTENDED via client.statement_execution
        # Parse: identify full scans, large shuffles, skew, missing predicate pushdown
        return f"explain_query_plan not yet implemented (query={query!r})"
