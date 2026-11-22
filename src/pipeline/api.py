"""Data ingestion API (FastAPI).

Endpoints
  GET  /health                 -> liveness check
  POST /ingest/file            -> upload a CSV; it is saved into the landing folder,
                                  where the file-watcher / Airflow sensor picks it up
  POST /stream/{source_key}    -> receive a micro-batch of JSON events (e.g. POS sales),
                                  validate each one and insert the valid ones into staging

Run:  uvicorn pipeline.api:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import shutil

from fastapi import FastAPI, File, HTTPException, UploadFile
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
def ingest_file(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv") or source_key_for_file(file.filename) is None:
        raise HTTPException(400, "File name must start with one of the 7 source prefixes (01_ .. 07_) and be .csv")
    tmp = LANDING_DIR / (file.filename + ".part")
    with tmp.open("wb") as fh:
        shutil.copyfileobj(file.file, fh)
    tmp.rename(LANDING_DIR / file.filename)  # atomic rename -> watcher never sees half a file
    return {"saved_to": str(LANDING_DIR / file.filename)}


@app.post("/stream/{source_key}")
def stream(source_key: str, batch: EventBatch):
    if source_key not in SOURCES:
        raise HTTPException(404, f"unknown source {source_key}")
    return insert_events(source_key, batch.load_id, batch.events)
