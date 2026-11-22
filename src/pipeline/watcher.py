"""Python file-watcher for the landing folder.

Two ways to use it:
  1. Inside Airflow: `landing_has_files()` is the poke function of a PythonSensor.
     It only reports a file once its size has stopped changing (so we never
     read a half-copied file).
  2. Stand-alone 24/7 daemon: `python -m pipeline.watcher --daemon`
     uses the `watchdog` library to react to new files instantly and triggers
     the Airflow batch-ingestion DAG through the Airflow REST API.
"""
from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

from .config import LANDING_DIR, STREAM_DIR, source_key_for_file

STABLE_SECONDS = 2


def _stable_files(folder: Path) -> list[Path]:
    files = [p for p in folder.glob("*.csv") if source_key_for_file(p.name)]
    if not files:
        return []
    sizes = {p: p.stat().st_size for p in files}
    time.sleep(STABLE_SECONDS)
    return sorted(p for p in files if p.exists() and p.stat().st_size == sizes[p])


def landing_has_files() -> bool:
    return bool(_stable_files(LANDING_DIR))


def stream_has_files() -> bool:
    return bool(_stable_files(STREAM_DIR))


def list_landing_files() -> list[str]:
    from .batch_ingest import order_files
    return [str(p) for p in order_files(_stable_files(LANDING_DIR))]


def list_stream_files() -> list[str]:
    return [str(p) for p in _stable_files(STREAM_DIR)]


def run_daemon():
    import requests
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer

    airflow = os.getenv("AIRFLOW_API", "http://airflow-webserver:8080/api/v1")
    auth = (os.getenv("AIRFLOW_USER", "admin"), os.getenv("AIRFLOW_PASSWORD", "admin"))

    class Handler(FileSystemEventHandler):
        def on_closed(self, event):  # file finished writing
            self._fire(event)

        def on_moved(self, event):
            self._fire(event, Path(event.dest_path))

        def _fire(self, event, path=None):
            path = path or Path(event.src_path)
            if event.is_directory or not source_key_for_file(path.name):
                return
            dag = "sdi_02_stream_ingestion" if path.parent == STREAM_DIR else "sdi_01_batch_ingestion"
            r = requests.post(f"{airflow}/dags/{dag}/dagRuns", json={"conf": {"trigger_file": path.name}}, auth=auth, timeout=10)
            print(f"[watcher] {path.name} -> triggered {dag}: HTTP {r.status_code}", flush=True)

    obs = Observer()
    for d in (LANDING_DIR, STREAM_DIR):
        obs.schedule(Handler(), str(d), recursive=False)
    obs.start()
    print(f"[watcher] watching {LANDING_DIR} and {STREAM_DIR}", flush=True)
    try:
        while True:
            time.sleep(1)
    finally:
        obs.stop()
        obs.join()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--daemon", action="store_true")
    if ap.parse_args().daemon:
        run_daemon()
    else:
        print(list_landing_files(), list_stream_files())
