"""DAG 1 - Batch ingestion: landing folder -> validation -> Postgres staging (ODS).

wait_for_files is the Python file-watcher (a sensor that only fires when files have
stopped growing). Master data is loaded first because sales/hospital validation
checks every MedicineID against it; the other files are loaded in parallel with
dynamic task mapping (one task instance per file).
"""
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.sensors.python import PythonSensor

from sdi_common import DEFAULT_ARGS, START, TAGS


@dag(dag_id="sdi_01_batch_ingestion", start_date=START, schedule=None, catchup=False,
     default_args=DEFAULT_ARGS, tags=TAGS + ["ingestion"], max_active_runs=1,
     params={"refresh_id": Param(1, type="integer", minimum=0)}, doc_md=__doc__)
def batch_ingestion():

    def _poke():
        from pipeline.watcher import landing_has_files
        return landing_has_files()

    wait_for_files = PythonSensor(task_id="wait_for_files", python_callable=_poke,
                                  poke_interval=15, timeout=60 * 30, mode="reschedule")

    @task
    def list_files() -> dict:
        from pipeline.watcher import list_landing_files
        files = list_landing_files()
        return {"master": [f for f in files if f.split("/")[-1].startswith("01_")],
                "others": [f for f in files if not f.split("/")[-1].startswith("01_")]}

    @task
    def ingest_master(files: dict) -> list:
        from pipeline.batch_ingest import ingest_file
        return [ingest_file(p) for p in files["master"]]

    @task
    def others(files: dict) -> list:
        return files["others"]

    @task(max_active_tis_per_dag=4)
    def ingest_one(path: str) -> dict:
        from pipeline.batch_ingest import ingest_file
        return ingest_file(path)

    @task
    def all_files_loaded():
        """Leaf gate: if any file failed, this ends upstream_failed so the whole run is marked
        FAILED (storage_metrics runs with all_done and would otherwise hide the failure)."""
        return True

    @task(trigger_rule="all_done")
    def storage_metrics():
        from pipeline.storage_metrics import collect
        collect()

    files = list_files()
    m = ingest_master(files)
    o = ingest_one.expand(path=others(files))
    wait_for_files >> files
    m >> o >> [storage_metrics(), all_files_loaded()]


batch_ingestion()
