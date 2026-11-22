#!/usr/bin/env bash
# Runs one full refresh by executing each child DAG with `airflow dags test`
# (useful without a scheduler; inside docker use the master DAG instead).
# usage: scripts/run_refresh_airflow_test.sh <refresh_id>
set -euo pipefail
RID=${1:?refresh id}
for DAG in sdi_00_generate_data sdi_01_batch_ingestion sdi_02_stream_ingestion sdi_03_transform_mart sdi_04_query_layer; do
  echo "=== refresh $RID : $DAG"
  airflow dags test "$DAG" -c "{\"refresh_id\": $RID}" 2>&1 | grep -E "Marking task as|Dag run|ERROR|Traceback" | grep -v "^$" || true
done
