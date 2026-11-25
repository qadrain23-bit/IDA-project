"""Data ingestion API (FastAPI).

Endpoints
  GET  /health                 -> liveness check
  POST /ingest/file?name=X.csv -> send a CSV as the raw request body; it is saved into the
                                  landing folder, where the file-watcher / Airflow sensor picks it up
                                  e.g. curl -X POST --data-binary @05_x.csv "localhost:8000/ingest/file?name=05_x.csv"

  POST /stream/{source_key}    -> receive a micro-batch of JSON events (e.g. POS sales),
                                  validate each one and insert the valid ones into staging

Run:  uvicorn pipeline.api:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from .config import LANDING_DIR, SOURCES, source_key_for_file
from .stream_ingest import insert_events

app = FastAPI(title="Pharma Cold Chain Ingestion API", version="1.0")


class EventBatch(BaseModel):
    load_id: int
    events: list[dict]


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/ingest/file")
async def ingest_file(name: str, request: Request):
    # raw body upload: no multipart dependency needed
    if "/" in name or "\\" in name or not name.endswith(".csv") or source_key_for_file(name) is None:
        raise HTTPException(400, "name must be a plain file name starting with 01_ .. 07_ and ending in .csv")
    tmp = LANDING_DIR / (name + ".part")
    with tmp.open("wb") as fh:
        async for chunk in request.stream():
            fh.write(chunk)
    tmp.rename(LANDING_DIR / name)  # atomic rename -> watcher never sees half a file
    return {"saved_to": str(LANDING_DIR / name)}


@app.post("/stream/{source_key}")
def stream(source_key: str, batch: EventBatch):
    if source_key not in SOURCES:
        raise HTTPException(404, f"unknown source {source_key}")
    return insert_events(source_key, batch.load_id, batch.events)
