"""MASTER DAG - one full refresh of the SDI mart, end to end.

generate data -> batch ingestion -> stream ingestion -> transform -> query layer

Trigger it with {"refresh_id": 0} for the initial historical load, then
{"refresh_id": 1} and {"refresh_id": 2} for the two refreshes (Jan and Feb 2026).
Each child DAG keeps its own tasks, logs and retries (modular design).
"""
from airflow.decorators import dag
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from sdi_common import DEFAULT_ARGS, START, TAGS

STEPS = ["sdi_00_generate_data", "sdi_01_batch_ingestion", "sdi_02_stream_ingestion",
         "sdi_03_transform_mart", "sdi_04_query_layer"]


@dag(dag_id="sdi_99_master_refresh", start_date=START, schedule=None, catchup=False,
     default_args={**DEFAULT_ARGS, "retries": 0}, tags=TAGS + ["master"], max_active_runs=1,
     params={"refresh_id": Param(1, type="integer", minimum=0)}, doc_md=__doc__)
def master_refresh():
    prev = None
    for dag_id in STEPS:
        t = TriggerDagRunOperator(
            task_id=f"run_{dag_id}",
            trigger_dag_id=dag_id,
            conf={"refresh_id": "{{ params.refresh_id }}"},
            wait_for_completion=True,
            poke_interval=10,
            reset_dag_run=True,
        )
        if prev:
            prev >> t
        prev = t


master_refresh()
