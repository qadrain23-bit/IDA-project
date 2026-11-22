"""Central configuration for the Pharma Cold Chain DWH pipeline.

Every path and connection setting lives here so DAGs and scripts never
hard-code them. Values can be overridden with environment variables
(docker-compose sets them for the Airflow containers).
"""
import os
from pathlib import Path

PROJECT_ROOT = Path(os.getenv("PIPELINE_HOME", Path(__file__).resolve().parents[2]))
DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", PROJECT_ROOT / "data"))

SOURCE_DIR = DATA_DIR / "source"        # original 7 CSV files (given by instructor)
LANDING_DIR = DATA_DIR / "landing"      # file-watcher drop zone for batch files
STREAM_DIR = DATA_DIR / "stream_in"     # files replayed as a real-time event stream
ARCHIVE_DIR = DATA_DIR / "archive"      # files move here after successful ingestion
WAREHOUSE_DIR = DATA_DIR / "warehouse"  # DuckDB data mart + parquet partitions
DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", WAREHOUSE_DIR / "sdi_mart.duckdb"))

PG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "dbname": os.getenv("PG_DB", "pharma_dwh"),
    "user": os.getenv("PG_USER", "pharma"),
    "password": os.getenv("PG_PASSWORD", "pharma"),
}

INGEST_API_URL = os.getenv("INGEST_API_URL", "http://localhost:8000")

# The 7 source domains. key -> (file name, staging table, primary key column(s), date column)
SOURCES = {
    "master":        ("01_master_data.csv",               "stg_master",        ["MedicineID"],                        "EffectiveFromDate"),
    "manufacturing": ("02_manufacturing_data.csv",        "stg_manufacturing", ["ManufacturingOrderID"],              "OrderDate"),
    "delivery":      ("03_manufacturer_delivery_data.csv", "stg_delivery",      ["ShipmentID", "ShipmentLineNumber"],  "DispatchDate"),
    "warehouse":     ("04_warehouse_data.csv",            "stg_warehouse",     ["InventorySnapshotID"],               "SnapshotDate"),
    "sales":         ("05_pharmacy_sales_data.csv",       "stg_sales",         ["SalesTransactionID"],                "SalesDate"),
    "hospital":      ("06_hospital_demand_data.csv",      "stg_hospital_demand", ["HospitalDemandRecordID"],          "DemandMonth"),
    "iot":           ("07_iot_streaming_data.csv",        "stg_iot",           ["SensorReadingID"],                   "EventTimestampUTC"),
}

# Phase 1 (Sales & Demand Intelligence) uses these three domains in the mart.
PHASE1_DOMAINS = ["master", "sales", "hospital"]


def source_key_for_file(filename: str) -> str | None:
    """Map a landed file name (e.g. 05_pharmacy_sales_data__r1.csv) to its domain key."""
    for key, (fname, *_rest) in SOURCES.items():
        if filename.startswith(fname[:2] + "_"):
            return key
    return None


for _d in (LANDING_DIR, STREAM_DIR, ARCHIVE_DIR, WAREHOUSE_DIR):
    _d.mkdir(parents=True, exist_ok=True)
