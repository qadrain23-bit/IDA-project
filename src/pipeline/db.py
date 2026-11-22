"""Small Postgres + DuckDB helpers shared by all pipeline steps."""
import io
from contextlib import contextmanager

import duckdb
import pandas as pd
import psycopg2

from .config import DUCKDB_PATH, PG


@contextmanager
def pg_conn():
    conn = psycopg2.connect(**PG)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def pg_execute(sql: str, params=None):
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)


def pg_query_df(sql: str, params=None) -> pd.DataFrame:
    with pg_conn() as conn:
        return pd.read_sql_query(sql, conn, params=params)


def copy_df_to_pg(conn, df: pd.DataFrame, table: str):
    """Fast bulk insert with Postgres COPY (much faster than row INSERTs)."""
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    cols = ",".join(f'"{c}"' for c in df.columns)
    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf
        )


def duck_conn(read_only: bool = False):
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DUCKDB_PATH), read_only=read_only)
