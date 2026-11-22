"""DAG 0 - Data oversampling layer.

refresh_id = 0 -> initial historical load: copies the 7 original files into the landing folder
refresh_id = N -> generates one new month of data for all 7 files (sales go to the stream folder)
"""
import shutil

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

from sdi_common import DEFAULT_ARGS, START, TAGS, refresh_id_from_context


@dag(dag_id="sdi_00_generate_data", start_date=START, schedule=None, catchup=False,
     default_args=DEFAULT_ARGS, tags=TAGS + ["oversampling"],
     params={"refresh_id": Param(1, type="integer", minimum=0)},
     doc_md=__doc__)
def generate_data():

    @task
    def init_postgres_schemas():
        from pipeline.batch_ingest import init_postgres
        init_postgres()

    @task
    def produce_files() -> dict:
        from pipeline.config import LANDING_DIR, SOURCE_DIR, SOURCES
        from pipeline.generator import generate
        rid = refresh_id_from_context(get_current_context())
        if rid == 0:
            copied = []
            for fname, *_ in SOURCES.values():
                shutil.copy(SOURCE_DIR / fname, LANDING_DIR / fname)
                copied.append(fname)
            return {"refresh_id": 0, "copied": copied}
        return generate(rid)

    @task
    def log_generation(stats: dict):
        import json
        from pipeline.db import pg_execute
        pg_execute("INSERT INTO ops.pipeline_run (refresh_id, step, started_at, ended_at, status, details)"
                   " VALUES (%s, 'generate_data', now(), now(), 'SUCCESS', %s)",
                   (stats["refresh_id"], json.dumps(stats, default=str)))

    init_postgres_schemas() >> log_generation(produce_files())


generate_data()
