"""Analytical query layer + query-performance monitoring.

Each business question (one future dashboard widget) has two versions:
  base      -> written against the star schema (fact + dimension joins)
  optimized -> reads the pre-aggregated summary tables (agg_*), DuckDB zone maps
               on the date-sorted fact, ART indexes, or the year/month parquet partitions
Both are timed with time.perf_counter() and logged to ops.query_log, which feeds
the Query Performance dashboard (execution time, query count, error rate).
"""
from __future__ import annotations

import time

from .db import duck_conn, pg_conn

# widget name -> (question, base SQL, optimized SQL)
QUERIES: dict[str, tuple[str, str, str]] = {
    "kpi_cards": (
        "Headline KPIs: revenue, margin, units, transactions (latest month vs all time)",
        """SELECT d.year_month, sum(f.net_sales_usd) revenue, sum(f.gross_margin_usd) margin,
                  sum(f.quantity_packs) units, count(*) txns
           FROM fact_sales f JOIN dim_date d ON d.date_key = f.sales_date_key
           GROUP BY 1 ORDER BY 1 DESC LIMIT 1""",
        """SELECT year_month, sum(net_sales_usd) revenue, sum(gross_margin_usd) margin,
                  sum(qty_packs) units, sum(txn_count) txns
           FROM agg_sales_monthly GROUP BY 1 ORDER BY 1 DESC LIMIT 1""",
    ),
    "channel_revenue_margin": (
        "Channel revenue split and gross margin % (Retail vs Hospital vs E-commerce)",
        """SELECT p.channel_group, sum(f.net_sales_usd) revenue, sum(f.gross_margin_usd) margin,
                  round(100 * sum(f.gross_margin_usd) / sum(f.net_sales_usd), 2) margin_pct,
                  round(100 * sum(f.net_sales_usd) / sum(sum(f.net_sales_usd)) OVER (), 2) revenue_share_pct
           FROM fact_sales f JOIN dim_sales_profile p ON p.sales_profile_key = f.sales_profile_key
           GROUP BY 1 ORDER BY revenue DESC""",
        """SELECT channel_group, sum(net_sales_usd) revenue, sum(gross_margin_usd) margin,
                  round(100 * sum(gross_margin_usd) / sum(net_sales_usd), 2) margin_pct,
                  round(100 * sum(net_sales_usd) / sum(sum(net_sales_usd)) OVER (), 2) revenue_share_pct
           FROM agg_sales_monthly GROUP BY 1 ORDER BY revenue DESC""",
    ),
    "monthly_revenue_trend": (
        "Monthly net revenue per channel (trend line)",
        """SELECT d.year_month, p.channel_group, sum(f.net_sales_usd) revenue
           FROM fact_sales f JOIN dim_date d ON d.date_key = f.sales_date_key
           JOIN dim_sales_profile p ON p.sales_profile_key = f.sales_profile_key
           GROUP BY 1, 2 ORDER BY 1, 2""",
        """SELECT year_month, channel_group, sum(net_sales_usd) revenue
           FROM agg_sales_monthly GROUP BY 1, 2 ORDER BY 1, 2""",
    ),
    "top10_fast_moving": (
        "Top 10 fast-moving medicines (units sold) within each therapeutic class",
        """SELECT * FROM (
             SELECT m.therapeutic_class, m.generic_name, sum(f.quantity_packs) units,
                    rank() OVER (PARTITION BY m.therapeutic_class ORDER BY sum(f.quantity_packs) DESC) rnk
             FROM fact_sales f JOIN dim_medicine m ON m.medicine_key = f.medicine_key
             GROUP BY 1, 2) WHERE rnk <= 10 ORDER BY units DESC LIMIT 10""",
        """SELECT * FROM (
             SELECT therapeutic_class, generic_name, sum(qty_packs) units,
                    rank() OVER (PARTITION BY therapeutic_class ORDER BY sum(qty_packs) DESC) rnk
             FROM agg_sales_monthly GROUP BY 1, 2) WHERE rnk <= 10 ORDER BY units DESC LIMIT 10""",
    ),
    "bottom10_slow_moving_cold_chain": (
        "10 slowest-moving cold-chain medicines in the last 6 months",
        """SELECT m.generic_name, m.therapeutic_class, sum(f.quantity_packs) units
           FROM fact_sales f JOIN dim_medicine m ON m.medicine_key = f.medicine_key
           JOIN dim_date d ON d.date_key = f.sales_date_key
           WHERE m.cold_chain_flag = 'Y'
             AND d.year_month > '{cutoff_6m}'
           GROUP BY 1, 2 ORDER BY units ASC LIMIT 10""",
        """SELECT generic_name, therapeutic_class, sum(qty_packs) units
           FROM agg_sales_monthly
           WHERE cold_chain_flag = 'Y'
             AND year_month > '{cutoff_6m}'
           GROUP BY 1, 2 ORDER BY units ASC LIMIT 10""",
    ),
    "forecast_vs_actual": (
        "Hospital forecast vs actual consumption ratio by month and therapeutic class",
        """SELECT d.year_month, m.therapeutic_class, sum(f.forecast_demand_packs) forecast,
                  sum(f.consumed_packs) consumed,
                  round(sum(f.consumed_packs) / nullif(sum(f.forecast_demand_packs), 0), 3) actual_to_forecast
           FROM fact_hospital_demand f JOIN dim_date d ON d.date_key = f.month_date_key
           JOIN dim_medicine m ON m.medicine_key = f.medicine_key
           GROUP BY 1, 2 ORDER BY 1, 2""",
        """SELECT year_month, therapeutic_class, sum(forecast_packs) forecast, sum(consumed_packs) consumed,
                  round(sum(consumed_packs) / nullif(sum(forecast_packs), 0), 3) actual_to_forecast
           FROM agg_demand_monthly GROUP BY 1, 2 ORDER BY 1, 2""",
    ),
    "unfulfilled_demand_by_hospital": (
        "Unfulfilled demand, stockout days and service level per hospital",
        """SELECT h.hospital_name, h.bed_size_band, sum(f.unfulfilled_demand_packs) unfulfilled,
                  sum(f.stockout_days) stockout_days, round(avg(f.service_level_pct), 2) service_level
           FROM fact_hospital_demand f JOIN dim_hospital h ON h.hospital_key = f.hospital_key
           GROUP BY 1, 2 ORDER BY unfulfilled DESC""",
        """SELECT a.hospital_name, h.bed_size_band, sum(a.unfulfilled_packs) unfulfilled,
                  sum(a.stockout_days) stockout_days, round(avg(a.avg_service_level_pct), 2) service_level
           FROM agg_demand_monthly a JOIN dim_hospital h ON h.hospital_id = a.hospital_id
           GROUP BY 1, 2 ORDER BY unfulfilled DESC""",
    ),
    "sales_vs_hospital_demand": (
        "Hospital-pharmacy sales vs hospital consumption per month (multi-fact drill-across)",
        """WITH s AS (SELECT d.year_month, sum(f.quantity_packs) sold
                     FROM fact_sales f JOIN dim_date d ON d.date_key = f.sales_date_key
                     WHERE f.hospital_key <> -1 GROUP BY 1),
                h AS (SELECT d.year_month, sum(f.consumed_packs) consumed
                     FROM fact_hospital_demand f JOIN dim_date d ON d.date_key = f.month_date_key GROUP BY 1)
           SELECT coalesce(s.year_month, h.year_month) year_month, s.sold, h.consumed
           FROM s FULL JOIN h ON s.year_month = h.year_month ORDER BY 1""",
        """WITH s AS (SELECT year_month, sum(qty_packs) sold FROM agg_sales_monthly
                     WHERE channel_group = 'Hospital' GROUP BY 1),
                h AS (SELECT year_month, sum(consumed_packs) consumed FROM agg_demand_monthly GROUP BY 1)
           SELECT coalesce(s.year_month, h.year_month) year_month, s.sold, h.consumed
           FROM s FULL JOIN h ON s.year_month = h.year_month ORDER BY 1""",
    ),
    "latest_month_partition_scan": (
        "Latest-month revenue by disease category (partition pruning demo)",
        """SELECT m.disease_category, sum(f.net_sales_usd) revenue
           FROM fact_sales f JOIN dim_medicine m ON m.medicine_key = f.medicine_key
           JOIN dim_date d ON d.date_key = f.sales_date_key
           WHERE d.year_month = '{latest_ym}'
           GROUP BY 1 ORDER BY 2 DESC""",
        """SELECT m.disease_category, sum(f.net_sales_usd) revenue
           FROM read_parquet('{parquet}/*/*/*.parquet', hive_partitioning = true) f
           JOIN dim_medicine m ON m.medicine_key = f.medicine_key
           WHERE f.year_num = {latest_year} AND f.month_num = {latest_month}   -- only 1 folder is read
           GROUP BY 1 ORDER BY 2 DESC""",
    ),
}


def _params(con) -> dict:
    """Values resolved once per run (latest month etc.) so filters are constants -> partition pruning works."""
    from .config import WAREHOUSE_DIR
    latest = con.execute("SELECT max(year_month) FROM agg_sales_monthly").fetchone()[0]
    y, m = (int(x) for x in latest.split("-"))
    cy, cm = (y, m - 6) if m > 6 else (y - 1, m + 6)
    return {"{parquet}": str(WAREHOUSE_DIR / "parquet" / "fact_sales"), "{latest_ym}": latest,
            "{latest_year}": str(y), "{latest_month}": str(m), "{cutoff_6m}": f"{cy}-{cm:02d}"}


def _fill(sql: str, params: dict) -> str:
    for k, v in params.items():
        sql = sql.replace(k, v)
    return sql


def run_query(con, sql: str):
    t0 = time.perf_counter()
    rows = con.execute(sql).fetchall()
    return rows, (time.perf_counter() - t0) * 1000


def benchmark(refresh_id: int | None = None, repeats: int = 5) -> list[dict]:
    """Run every widget query (base + optimized) `repeats` times and log each run."""
    con = duck_conn(read_only=True)
    logs, summary = [], []
    try:
        params = _params(con)
        for widget, (_question, base, opt) in QUERIES.items():
            for variant, sql in (("base", base), ("optimized", opt)):
                sql = _fill(sql, params)
                times = []
                for _ in range(repeats):
                    try:
                        rows, ms = run_query(con, sql)
                        times.append(ms)
                        logs.append((refresh_id, widget, widget, variant, round(ms, 3), len(rows), "SUCCESS", None))
                    except Exception as exc:  # counted in the Query Error Rate KPI
                        logs.append((refresh_id, widget, widget, variant, None, 0, "FAILED", str(exc)[:300]))
                summary.append({"widget": widget, "variant": variant,
                                "avg_ms": round(sum(times) / len(times), 2) if times else None,
                                "runs": repeats, "failed": repeats - len(times)})
    finally:
        con.close()
    with pg_conn() as conn, conn.cursor() as cur:
        cur.executemany("INSERT INTO ops.query_log (refresh_id, query_name, widget, variant, duration_ms,"
                        " rows_returned, status, error_message) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", logs)
    return summary


def results(widget: str, optimized: bool = True):
    """Return (columns, rows) for one widget - used by the Excel export and BI layer."""
    con = duck_conn(read_only=True)
    try:
        cur = con.execute(_fill(QUERIES[widget][2 if optimized else 1], _params(con)))
        return [d[0] for d in cur.description], cur.fetchall()
    finally:
        con.close()


if __name__ == "__main__":
    for row in benchmark():
        print(row)
