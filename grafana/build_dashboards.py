"""Builds the 4 Grafana observability dashboards (JSON) from one panel list.

Run:  python grafana/build_dashboards.py
Each panel's SQL reads only the ops.* tables in Postgres. The same PANELS list is
used by scripts/render_dashboard_previews.py to draw PNG previews for the report.
"""
import json
from pathlib import Path

DS = {"type": "grafana-postgresql-datasource", "uid": "pharma_pg"}
OUT = Path(__file__).parent / "dashboards"

# (title, panel type, sql, grid w, grid h, extra options)
DASHBOARDS = {
    "01_batch_ingestion": ("Batch Ingestion Monitoring", [
        ("Batch Success Rate (%)", "stat",
         "SELECT round(100.0 * count(*) FILTER (WHERE status = 'SUCCESS') / nullif(count(*), 0), 1) AS success_rate_pct FROM ops.batch_load_log",
         6, 4, {"unit": "percent"}),
        ("Files Loaded", "stat", "SELECT count(*) AS files FROM ops.batch_load_log", 6, 4, {}),
        ("Rows Loaded", "stat", "SELECT sum(rows_loaded) AS rows_loaded FROM ops.batch_load_log", 6, 4, {"unit": "short"}),
        ("Rows Rejected by Validation", "stat", "SELECT sum(rows_rejected) AS rows_rejected FROM ops.batch_load_log", 6, 4, {"color": "orange"}),
        ("Batch Load Duration (s) per Source File", "barchart",
         "SELECT file_name, duration_s FROM ops.batch_load_log WHERE status = 'SUCCESS' ORDER BY duration_s DESC",
         12, 10, {"orientation": "horizontal", "unit": "s"}),
        ("Rows Processed per Batch (loaded vs rejected)", "barchart",
         "SELECT 'R' || refresh_id || ' ' || source_key AS batch, rows_loaded, rows_rejected FROM ops.batch_load_log ORDER BY load_id",
         12, 10, {"orientation": "horizontal", "stacking": "normal", "log": True}),
        ("Rejected Rows by Reason", "barchart",
         "SELECT r.source_key || ': ' || r.reason AS reason, count(*) AS rows FROM staging.rejected_rows r JOIN ops.batch_load_log b USING (load_id) GROUP BY 1 ORDER BY 2 DESC",
         12, 8, {"orientation": "horizontal"}),
        ("Batch Load Log", "table",
         "SELECT load_id, refresh_id, source_key, file_name, file_size_mb, rows_read, rows_loaded, rows_rejected, duration_s, round(rows_loaded / nullif(duration_s, 0)) AS rows_per_sec, status, started_at FROM ops.batch_load_log ORDER BY load_id DESC",
         12, 8, {}),
    ]),
    "02_stream_ingestion": ("Streaming Ingestion Monitoring", [
        ("Throughput - latest run (events/s)", "stat",
         "SELECT throughput_eps FROM ops.stream_run ORDER BY load_id DESC LIMIT 1", 6, 4, {"unit": "short"}),
        ("Avg End-to-End Latency - latest run (s)", "stat",
         "SELECT avg_latency_s FROM ops.stream_run ORDER BY load_id DESC LIMIT 1", 6, 4, {"unit": "s", "decimals": 3}),
        ("Malformed Events (all runs)", "stat",
         "SELECT sum(events_malformed) AS malformed FROM ops.stream_run", 6, 4, {"color": "orange"}),
        ("Dropped Events (all runs)", "stat",
         "SELECT sum(events_dropped) AS dropped FROM ops.stream_run", 6, 4, {"color": "red"}),
        ("Stream Throughput (events per second) over the run - one line per refresh", "trend",
         "SELECT second_of_run, sum(events_loaded) FILTER (WHERE refresh_id = 1) AS refresh_1, sum(events_loaded) FILTER (WHERE refresh_id = 2) AS refresh_2, sum(events_loaded) FILTER (WHERE refresh_id = 3) AS refresh_3, sum(events_loaded) FILTER (WHERE refresh_id = 4) AS refresh_4 FROM (SELECT r.refresh_id, w.events_loaded, extract(epoch FROM w.window_start - min(w.window_start) OVER (PARTITION BY w.load_id))::int AS second_of_run FROM ops.stream_window w JOIN ops.stream_run r USING (load_id)) x GROUP BY 1 ORDER BY 1",
         24, 9, {"xField": "second_of_run", "unit": "short"}),
        ("End-to-End Latency per second of the run (avg, s)", "trend",
         "SELECT second_of_run, sum(avg_latency_s) FILTER (WHERE refresh_id = 1) AS refresh_1, sum(avg_latency_s) FILTER (WHERE refresh_id = 2) AS refresh_2, sum(avg_latency_s) FILTER (WHERE refresh_id = 3) AS refresh_3, sum(avg_latency_s) FILTER (WHERE refresh_id = 4) AS refresh_4 FROM (SELECT r.refresh_id, w.avg_latency_s, extract(epoch FROM w.window_start - min(w.window_start) OVER (PARTITION BY w.load_id))::int AS second_of_run FROM ops.stream_window w JOIN ops.stream_run r USING (load_id)) x GROUP BY 1 ORDER BY 1",
         12, 8, {"xField": "second_of_run", "unit": "s"}),
        ("Malformed Events per Run by Reason", "barchart",
         "SELECT 'Refresh ' || s.refresh_id || ': ' || r.reason AS reason, count(*) AS events FROM staging.rejected_rows r JOIN ops.stream_run s USING (load_id) GROUP BY 1 ORDER BY 1",
         12, 8, {"orientation": "horizontal"}),
        ("Stream Runs", "table",
         "SELECT load_id, refresh_id, file_name, events_sent, events_loaded, events_malformed, events_dropped, duration_s, throughput_eps, avg_latency_s, p95_latency_s, status FROM ops.stream_run ORDER BY load_id DESC",
         24, 6, {}),
    ]),
    "03_storage": ("Storage & Staging Monitoring", [
        ("Rows in Staging (Postgres)", "stat",
         "SELECT sum(row_count) AS rows FROM ops.storage_snapshot WHERE layer = 'staging_postgres' AND snapshot_at = (SELECT max(snapshot_at) FROM ops.storage_snapshot WHERE layer = 'staging_postgres')",
         6, 4, {"unit": "short"}),
        ("Rows in Data Mart (DuckDB)", "stat",
         "SELECT sum(row_count) AS rows FROM ops.storage_snapshot WHERE layer = 'mart_duckdb' AND snapshot_at = (SELECT max(snapshot_at) FROM ops.storage_snapshot WHERE layer = 'mart_duckdb')",
         6, 4, {"unit": "short"}),
        ("Postgres Database Size (MB)", "stat",
         "SELECT round(pg_database_size(current_database()) / 1e6, 1) AS mb", 6, 4, {"unit": "decmbytes"}),
        ("DuckDB Mart File Size (MB)", "stat",
         "SELECT sum(size_mb) AS mb FROM ops.storage_snapshot WHERE layer = 'mart_duckdb' AND snapshot_at = (SELECT max(snapshot_at) FROM ops.storage_snapshot WHERE layer = 'mart_duckdb')",
         6, 4, {"unit": "decmbytes"}),
        ("Storage Breakdown - % of staging rows by source domain", "piechart",
         "SELECT source_domain, row_count FROM ops.storage_snapshot WHERE layer = 'staging_postgres' AND snapshot_at = (SELECT max(snapshot_at) FROM ops.storage_snapshot WHERE layer = 'staging_postgres') ORDER BY row_count DESC",
         8, 10, {"pie": "donut"}),
        ("Storage Footprint (MB) per staging table", "barchart",
         "SELECT table_name, size_mb FROM ops.storage_snapshot WHERE layer = 'staging_postgres' AND snapshot_at = (SELECT max(snapshot_at) FROM ops.storage_snapshot WHERE layer = 'staging_postgres') ORDER BY size_mb DESC",
         8, 10, {"orientation": "horizontal", "unit": "decmbytes"}),
        ("Row Count Growth per Refresh (staging)", "barchart",
         "SELECT 'Refresh ' || refresh_id AS refresh, sum(row_count) AS rows FROM (SELECT DISTINCT ON (refresh_id, table_name) * FROM ops.storage_snapshot WHERE layer = 'staging_postgres' AND refresh_id IS NOT NULL ORDER BY refresh_id, table_name, snapshot_at DESC) s GROUP BY refresh_id ORDER BY refresh_id",
         8, 10, {"unit": "short"}),
        ("Date Coverage Span per Table (target: 2023-2025 fully present + new months)", "table",
         "SELECT layer, table_name, source_domain, row_count, min_date, max_date, (max_date - min_date) AS span_days, distinct_months, CASE WHEN min_date <= DATE '2023-01-31' AND max_date >= DATE '2025-12-01' THEN 'OK' WHEN min_date IS NULL THEN '-' ELSE 'GAP' END AS coverage_2023_2025 FROM ops.storage_snapshot WHERE snapshot_at IN (SELECT max(snapshot_at) FROM ops.storage_snapshot GROUP BY layer) ORDER BY layer DESC, row_count DESC",
         24, 10, {}),
    ]),
    "04_query_performance": ("Query Layer Performance", [
        ("Queries Executed (all time)", "stat", "SELECT count(*) AS queries FROM ops.query_log", 6, 4, {}),
        ("Query Error Rate (%)", "stat",
         "SELECT round(100.0 * count(*) FILTER (WHERE status = 'FAILED') / nullif(count(*), 0), 2) AS error_rate_pct FROM ops.query_log",
         6, 4, {"unit": "percent"}),
        ("Avg Time - star-schema queries (ms)", "stat",
         "SELECT round(avg(duration_ms), 2) AS ms FROM ops.query_log WHERE variant = 'base' AND status = 'SUCCESS'", 6, 4, {"unit": "ms"}),
        ("Avg Time - optimized queries (ms)", "stat",
         "SELECT round(avg(duration_ms), 2) AS ms FROM ops.query_log WHERE variant = 'optimized' AND status = 'SUCCESS'", 6, 4, {"unit": "ms", "color": "green"}),
        ("Average Query Execution Time (ms) per Dashboard Widget", "barchart",
         "SELECT widget, round(avg(duration_ms) FILTER (WHERE variant = 'base'), 2) AS base_star_schema_ms, round(avg(duration_ms) FILTER (WHERE variant = 'optimized'), 2) AS optimized_ms FROM ops.query_log WHERE status = 'SUCCESS' GROUP BY widget ORDER BY base_star_schema_ms DESC",
         24, 10, {"unit": "ms"}),
        ("Daily Active Query Count", "barchart",
         "SELECT to_char(run_at, 'YYYY-MM-DD') AS day, count(*) AS queries FROM ops.query_log GROUP BY 1 ORDER BY 1",
         8, 8, {}),
        ("Avg Execution Time per Refresh (ms)", "barchart",
         "SELECT 'Refresh ' || refresh_id AS refresh, round(avg(duration_ms) FILTER (WHERE variant = 'base'), 2) AS base_ms, round(avg(duration_ms) FILTER (WHERE variant = 'optimized'), 2) AS optimized_ms FROM ops.query_log WHERE status = 'SUCCESS' GROUP BY refresh_id ORDER BY refresh_id",
         8, 8, {"unit": "ms"}),
        ("Latest Query Runs", "table",
         "SELECT run_at, refresh_id, widget, variant, duration_ms, rows_returned, status, error_message FROM ops.query_log ORDER BY query_run_id DESC LIMIT 50",
         8, 8, {}),
    ]),
}


def panel_json(pid, title, ptype, sql, w, h, opts, x, y):
    fmt = opts.get("format", "table")
    p = {
        "id": pid, "title": title, "type": ptype, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [{"refId": "A", "datasource": DS, "editorMode": "code", "format": fmt,
                     "rawQuery": True, "rawSql": sql}],
        "fieldConfig": {"defaults": {"unit": opts.get("unit", "short")}, "overrides": []},
        "options": {},
    }
    if "decimals" in opts:
        p["fieldConfig"]["defaults"]["decimals"] = opts["decimals"]
    if ptype == "stat":
        p["options"] = {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "colorMode": "background", "graphMode": "none", "textMode": "value"}
        p["fieldConfig"]["defaults"]["color"] = {"mode": "fixed", "fixedColor": opts.get("color", "blue")}
    elif ptype == "barchart":
        p["options"] = {"orientation": opts.get("orientation", "vertical"), "showValue": "auto",
                        "stacking": opts.get("stacking", "none"), "legend": {"showLegend": True, "displayMode": "list", "placement": "bottom"},
                        "xTickLabelRotation": 0, "barWidth": 0.8, "groupWidth": 0.7}
        if opts.get("log"):
            p["fieldConfig"]["defaults"]["custom"] = {"scaleDistribution": {"type": "log", "log": 10}}
    elif ptype == "piechart":
        p["options"] = {"pieType": opts.get("pie", "pie"), "reduceOptions": {"calcs": ["lastNotNull"], "values": True},
                        "legend": {"showLegend": True, "displayMode": "table", "placement": "right", "values": ["percent", "value"]},
                        "displayLabels": ["percent"]}
    elif ptype == "trend":   # line chart with a numeric x axis (seconds since the stream started)
        p["fieldConfig"]["defaults"]["custom"] = {"drawStyle": "line", "lineWidth": 2, "showPoints": "always", "spanNulls": True}
        p["options"] = {"xField": opts["xField"], "legend": {"showLegend": True, "displayMode": "list", "placement": "bottom"}}
    elif ptype == "timeseries":
        p["fieldConfig"]["defaults"]["custom"] = {"drawStyle": "line", "lineWidth": 2, "showPoints": "auto"}
        p["options"] = {"legend": {"showLegend": True, "displayMode": "list", "placement": "bottom"}}
    elif ptype == "table":
        p["options"] = {"showHeader": True, "cellHeight": "sm"}
    return p


def build():
    OUT.mkdir(parents=True, exist_ok=True)
    for slug, (title, panels) in DASHBOARDS.items():
        x = y = row_h = 0
        items = []
        for i, (ptitle, ptype, sql, w, h, opts) in enumerate(panels, start=1):
            if x + w > 24:
                x, y, row_h = 0, y + row_h, 0
            items.append(panel_json(i, ptitle, ptype, sql, w, h, opts, x, y))
            x, row_h = x + w, max(row_h, h)
        dash = {"uid": slug, "title": f"SDI | {title}", "tags": ["pharma", "phase1", "observability"],
                "timezone": "browser", "schemaVersion": 39, "version": 1, "editable": True,
                "refresh": "10s", "time": {"from": "now-24h", "to": "now"}, "panels": items,
                "links": [{"title": "Other dashboards", "type": "dashboards", "tags": ["pharma"], "asDropdown": True}]}
        (OUT / f"{slug}.json").write_text(json.dumps(dash, indent=2))
        print("wrote", slug)


if __name__ == "__main__":
    build()
