"""DAG 4 - Analytical query layer: run every dashboard query (base + optimized),
time it and log it to ops.query_log; refresh the storage snapshot."""
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

from sdi_common import DEFAULT_ARGS, START, TAGS, refresh_id_from_context


@dag(dag_id="sdi_04_query_layer", start_date=START, schedule=None, catchup=False,
     default_args=DEFAULT_ARGS, tags=TAGS + ["query"], max_active_runs=1,
     params={"refresh_id": Param(1, type="integer", minimum=0)}, doc_md=__doc__)
def query_layer():

    @task
    def benchmark_queries() -> list:
        from pipeline.queries import benchmark
        return benchmark(refresh_id_from_context(get_current_context()), repeats=5)

    @task
    def storage_snapshot():
        from pipeline.storage_metrics import collect
        collect(refresh_id_from_context(get_current_context()))

    benchmark_queries() >> storage_snapshot()


query_layer()
