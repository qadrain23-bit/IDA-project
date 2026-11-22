"""Streaming ingestion layer.

New point-of-sale transactions are treated as a live event stream:
  producer (replay_stream_file) --HTTP micro-batches--> ingestion API --> staging.stg_sales

Metrics for the Streaming Ingestion dashboard:
  * Throughput (events/s)     = events loaded / stream duration, also per 1-second window
  * End-to-end latency (s)    = time row committed in Postgres (_ingested_at)
                                - time the producer created the event (_event_created_at)
  * Malformed / dropped count = rows failing validation (try/except counter) /
                                rows lost because an API call failed after retries
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from psycopg2.extras import execute_values

from .config import ARCHIVE_DIR, INGEST_API_URL, SOURCES, source_key_for_file
from .db import copy_df_to_pg, pg_conn
from .validation import known_medicines, rejected_to_rows, validate

_MEDS_CACHE: dict = {}


def insert_events(source_key: str, load_id: int, events: list[dict]) -> dict:
    """Validate a micro-batch of events and insert the good ones (called by the API)."""
    _fname, table, pk, _ = SOURCES[source_key]
    df = pd.DataFrame(events)
    created = df.pop("_event_created_at") if "_event_created_at" in df else None
    if "meds" not in _MEDS_CACHE or time.time() - _MEDS_CACHE["t"] > 60:
        _MEDS_CACHE.update(meds=known_medicines(), t=time.time())
    malformed = 0
    try:
        df = df.replace({"": None})
        clean, rejected = validate(df, source_key, table, pk, _MEDS_CACHE["meds"])
        malformed = len(rejected)
    except Exception as exc:  # whole micro-batch unparseable -> count all as malformed
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute("INSERT INTO staging.rejected_rows (load_id, source_key, reason) VALUES (%s,%s,%s)",
                        (load_id, source_key, f"unparseable batch: {exc}"[:300]))
        return {"loaded": 0, "malformed": len(events)}
    if created is not None:
        clean = clean.assign(_event_created_at=created.loc[clean.index].values)
    clean = clean.assign(_load_id=load_id, _source_file="stream")
    with pg_conn() as conn:
        if len(clean):
            copy_df_to_pg(conn, clean, f"staging.{table}")
        if malformed:
            with conn.cursor() as cur:
                execute_values(cur,
                    "INSERT INTO staging.rejected_rows (load_id, source_key, record_key, reason, raw_record) VALUES %s",
                    rejected_to_rows(rejected, load_id, source_key, pk))
    return {"loaded": len(clean), "malformed": malformed}


def _post(source_key, payload, direct: bool, retries=3):
    if direct:  # same code path without HTTP (used for local tests when the API is not running)
        return insert_events(source_key, payload["load_id"], payload["events"])
    for attempt in range(retries):
        try:
            r = requests.post(f"{INGEST_API_URL}/stream/{source_key}", json=payload, timeout=30)
            r.raise_for_status()
            return r.json()
        except requests.RequestException:
            time.sleep(0.5 * (attempt + 1))
    return None  # dropped


def replay_stream_file(path: str | Path, events_per_second: int = 400, micro_batch: int = 50,
                       direct: bool = False, archive: bool = True) -> dict:
    """Send a file row-by-row (in small micro-batches) at a fixed rate, like a live POS feed."""
    path = Path(path)
    key = source_key_for_file(path.name)
    refresh_id = int(path.name.split("__r")[1].split("_")[0]) if "__r" in path.name else 0
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
    df = df.astype(object).where(df.notna(), None)

    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT nextval('ops.load_id_seq')")
        load_id = cur.fetchone()[0]
        started = datetime.now(timezone.utc)
        cur.execute("INSERT INTO ops.stream_run (load_id, refresh_id, source_key, file_name, started_at, status)"
                    " VALUES (%s,%s,%s,%s,%s,'RUNNING')", (load_id, refresh_id, key, path.name, started))

    sent = loaded = malformed = dropped = 0
    interval = micro_batch / events_per_second
    t0 = time.perf_counter()
    records = df.to_dict("records")
    for i in range(0, len(records), micro_batch):
        chunk = records[i:i + micro_batch]
        now = datetime.now(timezone.utc).isoformat()
        for ev in chunk:
            ev["_event_created_at"] = now   # event "created" at the source right now
        res = _post(key, {"load_id": load_id, "events": chunk}, direct)
        sent += len(chunk)
        if res is None:
            dropped += len(chunk)
        else:
            loaded += res["loaded"]
            malformed += res["malformed"]
        sleep_for = (i // micro_batch + 1) * interval - (time.perf_counter() - t0)
        if sleep_for > 0:
            time.sleep(sleep_for)
    duration = time.perf_counter() - t0

    table = SOURCES[key][1]
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO ops.stream_window (load_id, window_start, events_loaded, avg_latency_s, max_latency_s)
            SELECT %(id)s, date_trunc('second', _ingested_at), count(*),
                   avg(extract(epoch FROM _ingested_at - _event_created_at)),
                   max(extract(epoch FROM _ingested_at - _event_created_at))
            FROM staging.{table} WHERE _load_id = %(id)s GROUP BY 2
            ON CONFLICT DO NOTHING""", {"id": load_id})
        cur.execute("""
            UPDATE ops.stream_window w SET events_malformed = r.n
            FROM (SELECT date_trunc('second', rejected_at) ws, count(*) n FROM staging.rejected_rows
                  WHERE load_id = %(id)s GROUP BY 1) r
            WHERE w.load_id = %(id)s AND w.window_start = r.ws""", {"id": load_id})
        cur.execute(f"""
            SELECT avg(extract(epoch FROM _ingested_at - _event_created_at)),
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM _ingested_at - _event_created_at))
            FROM staging.{table} WHERE _load_id = %s""", (load_id,))
        avg_lat, p95 = cur.fetchone()
        cur.execute("""UPDATE ops.stream_run SET ended_at=%s, duration_s=%s, events_sent=%s, events_loaded=%s,
                       events_malformed=%s, events_dropped=%s, throughput_eps=%s, avg_latency_s=%s,
                       p95_latency_s=%s, status=%s WHERE load_id=%s""",
                    (datetime.now(timezone.utc), round(duration, 3), sent, loaded, malformed, dropped,
                     round(loaded / duration, 2) if duration else None, avg_lat, p95,
                     "SUCCESS" if dropped == 0 else "PARTIAL", load_id))
    if archive:
        path.rename(ARCHIVE_DIR / path.name)
    return {"load_id": load_id, "file": path.name, "sent": sent, "loaded": loaded, "malformed": malformed,
            "dropped": dropped, "duration_s": round(duration, 2),
            "throughput_eps": round(loaded / duration, 1) if duration else None,
            "avg_latency_s": float(avg_lat) if avg_lat is not None else None}
