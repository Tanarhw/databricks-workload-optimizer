import os

from databricks.sdk import WorkspaceClient


def get_client() -> WorkspaceClient:
    """Authenticated Databricks client — PAT locally, auto-auth when deployed as a Databricks App."""
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")

    if host and token:
        return WorkspaceClient(host=host, token=token)

    # Databricks Apps runtime injects credentials automatically
    return WorkspaceClient()
