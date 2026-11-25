"""Row-level data validation used by both the batch and the streaming path.

Two kinds of checks:
  1. Schema checks (all 7 files): primary key not null, dates parse,
     numbers parse, whole-number columns really are whole numbers.
  2. Business rules (Phase 1 files):
       sales    -> price > 0, quantity > 0, valid sale date, non-null SKU,
                   SKU exists in the medicine master
       hospital -> valid month, non-null hospital and SKU, quantities >= 0
       master   -> retail price > 0, min temp <= max temp
Rows that fail are not loaded; they go to staging.rejected_rows with the reason.
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

from .db import pg_conn

_TYPE_CACHE: dict[str, dict[str, str]] = {}


def staging_types(table: str) -> dict[str, str]:
    """Column -> Postgres data type, read from the live staging table."""
    if table not in _TYPE_CACHE:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema='staging' AND table_name=%s ORDER BY ordinal_position",
                (table,),
            )
            _TYPE_CACHE[table] = {c: t for c, t in cur.fetchall() if not c.startswith("_")}
    return _TYPE_CACHE[table]


def known_medicines() -> set[str]:
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute('SELECT DISTINCT "MedicineID" FROM staging.stg_master')
        return {r[0] for r in cur.fetchall()}


def validate(df: pd.DataFrame, source_key: str, table: str, pk: list[str],
             medicines: set[str] | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (clean typed rows, rejected rows with a 'reason' column)."""
    types = staging_types(table)
    df = df[[c for c in types if c in df.columns]].copy()
    original = df.copy()
    reason = pd.Series("", index=df.index, dtype=object)

    def flag(mask, text):
        mask = mask.fillna(True) if hasattr(mask, "fillna") else mask
        reason[mask & (reason == "")] = text

    for k in pk:
        flag(df[k].isna() | (df[k].astype(str).str.strip() == ""), f"null primary key {k}")

    for col, typ in types.items():
        if col not in df.columns:
            continue
        raw = df[col]
        present = raw.notna() & (raw.astype(str).str.strip() != "")
        if typ == "date":
            parsed = pd.to_datetime(raw, errors="coerce", format="%Y-%m-%d")
            flag(present & parsed.isna(), f"invalid date in {col}")
            df[col] = parsed.dt.strftime("%Y-%m-%d")
        elif typ.startswith("timestamp"):
            parsed = pd.to_datetime(raw, errors="coerce", utc=True)
            flag(present & parsed.isna(), f"invalid timestamp in {col}")
            df[col] = parsed.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        elif typ in ("bigint", "integer", "numeric"):
            num = pd.to_numeric(raw, errors="coerce")
            flag(present & num.isna(), f"non-numeric {col}")
            if typ != "numeric":
                flag(num.notna() & (num % 1 != 0), f"non-integer {col}")
                df[col] = num.round().astype("Int64")
            else:
                df[col] = num

    if source_key == "sales":
        flag(df["MedicineID"].isna(), "null MedicineID (SKU)")
        flag(~(df["UnitRetailPriceUSD"] > 0), "price <= 0")
        flag(~(df["QuantitySoldPacks"] > 0), "quantity <= 0")
        if medicines:
            flag(df["MedicineID"].notna() & ~df["MedicineID"].isin(medicines), "unknown MedicineID")
    elif source_key == "hospital":
        flag(df["MedicineID"].isna(), "null MedicineID (SKU)")
        flag(df["HospitalID"].isna(), "null HospitalID")
        qty_cols = [c for c in df.columns if c.endswith("Packs")]
        neg = np.zeros(len(df), dtype=bool)
        for c in qty_cols:
            neg |= (df[c] < 0).fillna(False).astype(bool).to_numpy()
        flag(pd.Series(neg, index=df.index), "negative quantity")
    elif source_key == "master":
        flag(~(df["RecommendedRetailPriceUSD"] > 0), "retail price <= 0")
        flag(df["StorageMinTempC"] > df["StorageMaxTempC"], "min temp > max temp")

    bad = reason != ""
    rejected = original[bad].copy()
    rejected["reason"] = reason[bad]
    return df[~bad], rejected


def rejected_to_rows(rejected: pd.DataFrame, load_id: int, source_key: str, pk: list[str]):
    rows = []
    for _, r in rejected.iterrows():
        raw = {k: (None if pd.isna(v) else str(v)) for k, v in r.drop("reason").items()}
        key = "|".join(str(raw.get(k)) for k in pk)
        rows.append((load_id, source_key, key, r["reason"], json.dumps(raw)))
    return rows
