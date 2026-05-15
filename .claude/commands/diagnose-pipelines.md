Run a full diagnostic sweep of all Databricks pipelines configured in `config/pipelines.yaml`.

Steps:
1. Run `uv run python scripts/run_diagnostics.py` from the project root and capture the output.
2. Display the full report output to the user.
3. If there are HIGH severity issues, call them out clearly and ask the user if they want to dig into any specific resource.
4. If the user asks to investigate a specific job, cluster, or table further, use the relevant tool or read the relevant file to provide deeper analysis.
5. If the exit code is 0 (no HIGH issues), confirm everything looks healthy.

Notes:
- The script reads Databricks credentials from `.env` automatically.
- To add or remove pipelines from the sweep, edit `config/pipelines.yaml`.
- To schedule this weekly, run `/schedule` and set the prompt to `/diagnose-pipelines`.
