from fastapi import FastAPI
from fastmcp import FastMCP

from server.tools import register_tools

mcp = FastMCP("Databricks Workload Optimizer")
register_tools(mcp)

# Streamable HTTP transport — required for Databricks Apps / Marketplace
app = FastAPI(title="Databricks Workload Optimizer")
app.mount("/mcp", mcp.http_app())


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
