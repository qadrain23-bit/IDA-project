"""DAG 3 - Transformation & dimensional modeling: staging -> DuckDB SDI star schema.

Incremental (watermark per source) so new data is appended to history:
SCD Type 2 on dim_medicine, Type 1 on pharmacy/hospital, junk dimensions,
two fact tables, then summary tables + year/month parquet partitions.
"""
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

from sdi_common import DEFAULT_ARGS, START, TAGS, refresh_id_from_context


@dag(dag_id="sdi_03_transform_mart", start_date=START, schedule=None, catchup=False,
     default_args=DEFAULT_ARGS, tags=TAGS + ["transformation"], max_active_runs=1,
     params={"refresh_id": Param(1, type="integer", minimum=0)}, doc_md=__doc__)
def transform_mart():

    @task
    def build_star_schema() -> dict:
        from pipeline.transform import run_transform
        return run_transform(refresh_id_from_context(get_current_context()))

    @task
    def check_mart(stats: dict):
        """Data-quality gate: fail the run if the mart lost rows or has orphan keys."""
        from pipeline.db import duck_conn
        con = duck_conn(read_only=True)
        try:
            orphans = con.execute("""SELECT count(*) FROM fact_sales f
                                     LEFT JOIN dim_medicine m USING (medicine_key)
                                     WHERE m.medicine_key IS NULL""").fetchone()[0]
            current = con.execute("""SELECT count(*) FROM (SELECT medicine_id FROM dim_medicine
                                     WHERE is_current GROUP BY 1 HAVING count(*) > 1)""").fetchone()[0]
        finally:
            con.close()
        if orphans or current:
            raise ValueError(f"mart check failed: orphan facts={orphans}, medicines with 2 current rows={current}")
        return stats

    check_mart(build_star_schema())


transform_mart()
