#!/usr/bin/env bash
# Start Airflow (standalone) from its own virtualenv. Safe to run again: it stops the old one first.
set -euo pipefail
REPO=/workspaces/Warranty-lakehouse
export AIRFLOW_HOME="$HOME/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$REPO/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
mkdir -p "$AIRFLOW_HOME"
pkill -f "airflow standalone" 2>/dev/null || true
export AIRFLOW_CONN_DATABRICKS_DEFAULT="$(python3 - <<'PY'
import json, os, re
host = re.sub(r"^https?://", "", os.environ["DATABRICKS_HOST"]).split("/")[0]
print(json.dumps({"conn_type": "databricks", "host": host, "password": os.environ["DATABRICKS_TOKEN"]}))
PY
)"
nohup "$HOME/airflow_venv/bin/airflow" standalone > "$AIRFLOW_HOME/standalone.log" 2>&1 &
echo "Airflow starting. Open port 8080 in about a minute. Log: $AIRFLOW_HOME/standalone.log"