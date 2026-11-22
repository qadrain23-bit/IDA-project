"""Batch ingestion layer: landing folder -> validation -> Postgres staging (ODS).

For every file it records the metrics the Batch Ingestion dashboard needs:
  * Batch Load Duration (s) = ended_at - started_at  (read + parse + validate + insert)
  * Rows Processed           = len(df) read, rows loaded, rows rejected
  * Batch Success            = status SUCCESS / FAILED (success rate = SUCCESS / all * 100)
"""
from __future__ import annotations

import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from psycopg2.extras import execute_values

from .config import ARCHIVE_DIR, PROJECT_ROOT, SOURCES, source_key_for_file
from .db import copy_df_to_pg, pg_conn
from .validation import known_medicines, rejected_to_rows, validate

SQL_DIR = Path(__file__).parent / "sql"


def init_postgres():
    """Create the ops + staging schemas (safe to run many times)."""
    with pg_conn() as conn, conn.cursor() as cur:
        for f in ("00_ops_ddl.sql", "01_staging_ddl.sql"):
            cur.execute((SQL_DIR / f).read_text())


def next_load_id(cur) -> int:
    cur.execute("SELECT nextval('ops.load_id_seq')")
    return cur.fetchone()[0]


def _refresh_from_name(name: str) -> int:
    # files from the generator look like 05_..._data__r2_202602.csv ; originals are refresh 0
    if "__r" in name:
        return int(name.split("__r")[1].split("_")[0])
    return 0


def ingest_file(path: str | Path, archive: bool = True) -> dict:
    path = Path(path)
    key = source_key_for_file(path.name)
    if key is None:
        raise ValueError(f"Unknown source file: {path.name}")
    fname, table, pk, _anchor = SOURCES[key]
    refresh_id = _refresh_from_name(path.name)
    started = datetime.now(timezone.utc)
    t0 = time.perf_counter()

    with pg_conn() as conn, conn.cursor() as cur:
        load_id = next_load_id(cur)
        cur.execute(
            "INSERT INTO ops.batch_load_log (load_id, refresh_id, source_key, file_name, file_size_mb,"
            " started_at, status) VALUES (%s,%s,%s,%s,%s,%s,'RUNNING')",
            (load_id, refresh_id, key, path.name, round(path.stat().st_size / 1e6, 3), started),
        )

    result = {"load_id": load_id, "source_key": key, "file": path.name, "refresh_id": refresh_id}
    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
        rows_read = len(df)
        meds = known_medicines() if key in ("sales", "hospital") else None
        clean, rejected = validate(df, key, table, pk, meds)
        clean = clean.assign(_load_id=load_id, _source_file=path.name)
        with pg_conn() as conn:
            copy_df_to_pg(conn, clean, f"staging.{table}")
            if len(rejected):
                with conn.cursor() as cur:
                    execute_values(cur,
                        "INSERT INTO staging.rejected_rows (load_id, source_key, record_key, reason, raw_record) VALUES %s",
                        rejected_to_rows(rejected, load_id, key, pk))
        duration = time.perf_counter() - t0
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE ops.batch_load_log SET ended_at=%s, duration_s=%s, rows_read=%s, rows_loaded=%s,"
                " rows_rejected=%s, status='SUCCESS' WHERE load_id=%s",
                (datetime.now(timezone.utc), round(duration, 3), rows_read, len(clean), len(rejected), load_id),
            )
        if archive and ARCHIVE_DIR not in path.parents:
            shutil.move(str(path), ARCHIVE_DIR / path.name)
        result.update(status="SUCCESS", rows_read=rows_read, rows_loaded=len(clean),
                      rows_rejected=len(rejected), duration_s=round(duration, 3))
    except Exception as exc:  # recorded for the success-rate KPI, then re-raised for Airflow retries
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE ops.batch_load_log SET ended_at=%s, duration_s=%s, status='FAILED', error_message=%s"
                " WHERE load_id=%s",
                (datetime.now(timezone.utc), round(time.perf_counter() - t0, 3), str(exc)[:500], load_id),
            )
        raise
    return result


def order_files(paths: list[Path]) -> list[Path]:
    """Master data first (sales/hospital validation looks up the medicine list)."""
    return sorted(paths, key=lambda p: (0 if p.name.startswith("01_") else 1, p.name))


if __name__ == "__main__":
    import sys
    init_postgres()
    for p in order_files([Path(a) for a in sys.argv[1:]]):
        print(ingest_file(p, archive=False))
