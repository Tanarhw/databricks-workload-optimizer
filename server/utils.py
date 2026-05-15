import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, StatementState


def get_client() -> WorkspaceClient:
    """Authenticated Databricks client — PAT locally, auto-auth when deployed as a Databricks App."""
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")

    if host and token:
        return WorkspaceClient(host=host, token=token)

    # Databricks Apps runtime injects credentials automatically
    return WorkspaceClient()


def get_warehouse_id(client: WorkspaceClient) -> str:
    """Return the ID of the first running SQL warehouse, or the first available one."""
    warehouses = list(client.warehouses.list())
    running = [w for w in warehouses if w.state and w.state.value == "RUNNING"]
    candidates = running or warehouses
    if not candidates or not candidates[0].id:
        raise RuntimeError("No SQL warehouses found in this workspace.")
    return candidates[0].id


def run_sql(client: WorkspaceClient, warehouse_id: str, statement: str) -> list[dict]:
    """Execute SQL and return rows as a list of dicts keyed by column name."""
    resp = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
        disposition=Disposition.INLINE,
    )
    if resp.status.state != StatementState.SUCCEEDED:
        err = resp.status.error
        raise RuntimeError(f"SQL failed ({resp.status.state}): {err}")

    if not resp.result or not resp.result.data_array:
        return []

    columns = [col.name for col in (resp.manifest.schema.columns or [])]
    return [dict(zip(columns, row)) for row in resp.result.data_array]
