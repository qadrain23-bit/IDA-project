"""DAG 2 - Streaming ingestion of point-of-sale transactions.

Replays the new month of sales as a live event feed: the producer sends small
micro-batches (50 events) at a fixed rate to the ingestion API, which validates
each event and commits it to staging.stg_sales. Throughput, end-to-end latency
and malformed counts are written to ops.stream_run / ops.stream_window.
Skips itself when there is no stream file (e.g. the initial historical load).
"""
import os

from airflow.decorators import dag, task
from airflow.models.param import Param

from sdi_common import DEFAULT_ARGS, START, TAGS

EVENTS_PER_SECOND = int(os.getenv("STREAM_EVENTS_PER_SECOND", "400"))


@dag(dag_id="sdi_02_stream_ingestion", start_date=START, schedule=None, catchup=False,
     default_args=DEFAULT_ARGS, tags=TAGS + ["ingestion", "streaming"], max_active_runs=1,
     params={"refresh_id": Param(1, type="integer", minimum=0)}, doc_md=__doc__)
def stream_ingestion():

    @task.short_circuit
    def stream_file_present() -> bool:
        from pipeline.watcher import stream_has_files
        return stream_has_files()

    @task
    def api_healthy():
        import requests
        from pipeline.config import INGEST_API_URL
        requests.get(f"{INGEST_API_URL}/health", timeout=10).raise_for_status()

    @task
    def replay_stream() -> list:
        from pipeline.stream_ingest import replay_stream_file
        from pipeline.watcher import list_stream_files
        return [replay_stream_file(p, events_per_second=EVENTS_PER_SECOND) for p in list_stream_files()]

    @task
    def storage_metrics():
        from pipeline.storage_metrics import collect
        collect()

    stream_file_present() >> api_healthy() >> replay_stream() >> storage_metrics()


stream_ingestion()
