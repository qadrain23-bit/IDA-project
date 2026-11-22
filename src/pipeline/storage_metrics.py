"""Storage & staging metrics: row counts, size (MB) and date coverage per table.

Postgres runs in its own container, so its size is read with
pg_total_relation_size() (the database's own on-disk size for that table)
instead of os.path.getsize(). The DuckDB mart is a single file, so its size
is os.path.getsize(DUCKDB_PATH).
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

from .config import DUCKDB_PATH, SOURCES
from .db import duck_conn, pg_conn

MART_TABLES = {  # table -> (domain, date expression)
    "fact_sales": ("sales", "strptime(sales_date_key::VARCHAR, '%Y%m%d')"),
    "fact_hospital_demand": ("hospital", "strptime(month_date_key::VARCHAR, '%Y%m%d')"),
    "dim_medicine": ("master", None),
    "dim_pharmacy": ("sales", None),
    "dim_hospital": ("hospital", None),
    "dim_sales_profile": ("sales", None),
    "dim_demand_status": ("hospital", None),
    "dim_date": ("conformed", "full_date"),
    "agg_sales_monthly": ("sales", None),
    "agg_demand_monthly": ("hospital", None),
}


def collect(refresh_id: int | None = None) -> list[tuple]:
    now = datetime.now(timezone.utc)
    rows = []
    with pg_conn() as conn, conn.cursor() as cur:
        for key, (_f, table, _pk, date_col) in SOURCES.items():
            cur.execute(f"""SELECT count(*), pg_total_relation_size('staging.{table}') / 1e6,
                                   min("{date_col}")::date, max("{date_col}")::date,
                                   count(DISTINCT date_trunc('month', "{date_col}"))
                            FROM staging.{table}""")
            n, mb, dmin, dmax, months = cur.fetchone()
            rows.append((now, refresh_id, "staging_postgres", table, key, n, round(float(mb), 3), dmin, dmax, months))
    total_duck_mb = os.path.getsize(DUCKDB_PATH) / 1e6 if DUCKDB_PATH.exists() else 0
    con = duck_conn(read_only=True)
    try:
        counts = {}
        for t in MART_TABLES:
            try:
                counts[t] = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
            except Exception:
                counts[t] = 0
        total_rows = sum(counts.values()) or 1
        for t, (domain, dexpr) in MART_TABLES.items():
            dmin = dmax = months = None
            if dexpr and counts[t]:
                src = "fact_sales" if t == "dim_date" else t
                q = (f"SELECT min({dexpr})::DATE, max({dexpr})::DATE, count(DISTINCT date_trunc('month', {dexpr})) FROM {t}"
                     if t != "dim_date" else
                     f"SELECT min(full_date), max(full_date), count(DISTINCT year_month) FROM dim_date "
                     f"WHERE date_key IN (SELECT DISTINCT sales_date_key FROM {src})")
                dmin, dmax, months = con.execute(q).fetchone()
            # DuckDB stores everything in one file: share of file size by share of rows (approximation)
            mb = round(total_duck_mb * counts[t] / total_rows, 3)
            rows.append((now, refresh_id, "mart_duckdb", t, domain, counts[t], mb, dmin, dmax, months))
    finally:
        con.close()
    with pg_conn() as conn, conn.cursor() as cur:
        cur.executemany("INSERT INTO ops.storage_snapshot VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", rows)
    return rows


if __name__ == "__main__":
    for r in collect():
        print(r[2:])
