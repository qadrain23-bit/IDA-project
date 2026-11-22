"""Shared settings for all SDI DAGs (not a DAG itself)."""
import os
import sys
from datetime import datetime, timedelta

# make the pipeline package importable inside the Airflow containers
SRC = os.getenv("PIPELINE_SRC", os.path.join(os.path.dirname(__file__), "..", "src"))
if SRC not in sys.path:
    sys.path.insert(0, SRC)

DEFAULT_ARGS = {
    "owner": "team-sdi",
    "retries": 2,                         # automatic retry policy
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=30),
}
START = datetime(2026, 11, 1)
TAGS = ["phase1", "sdi"]


def refresh_id_from_context(context) -> int:
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    return int(conf.get("refresh_id", context["params"].get("refresh_id", 1)))
