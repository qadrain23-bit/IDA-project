"""Data oversampling layer.

Creates one new month of realistic data for each of the 7 source files, so the
pipeline can be refreshed and the dashboards can be seen changing.

Method (short version, full reasoning is in docs/Data_Generation_Report.pdf):
  * Volume  : rows per refresh = median monthly volume of the last 12 months
              (median, not mean, because some months - e.g. Jan 2025 sales -
              are big outliers).
  * Content : "time-shifted bootstrap". Real rows from the last 12 months are
              sampled and moved into the new month. All dates inside a row are
              moved by the same number of days, so gaps such as
              order->delivery stay realistic. Expiry dates are NOT moved,
              because expiry belongs to the physical batch.
  * Keys    : every new row gets a new primary key that continues the
              existing sequence (SALE-000500001, ...).
  * Fix-ups : money and stock columns are recomputed so totals stay correct
              (net = gross - discount, closing = opening + received - ...).
  * Master  : the full 63-SKU master is re-sent with a few price changes, so
              the warehouse can show Slowly Changing Dimension (Type 2) history.
  * Noise   : a small share of deliberately broken rows (price <= 0, missing
              MedicineID, impossible dates) is added so the validation step has
              something real to catch.
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import date

import numpy as np
import pandas as pd

from .config import DATA_DIR, LANDING_DIR, SOURCE_DIR, SOURCES, STREAM_DIR

GENERATED_DIR = DATA_DIR / "generated"     # history of everything generated
STATS_FILE = GENERATED_DIR / "generation_stats.json"

SALES_BAD_ROW_SHARE = 0.004      # 0.4 % malformed sales events
HOSPITAL_BAD_ROW_SHARE = 0.003   # 0.3 % malformed hospital rows
OTHER_BAD_ROW_SHARE = 0.002


# --------------------------------------------------------------------------- helpers
def _read(path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])


def _history(key: str) -> pd.DataFrame:
    """Original file + every month generated so far (newest data matters)."""
    fname = SOURCES[key][0]
    frames = [_read(SOURCE_DIR / fname)]
    for p in sorted(GENERATED_DIR.glob(f"{fname[:-4]}__r*.csv")):
        frames.append(_read(p))
    return pd.concat(frames, ignore_index=True)


def _date_columns(df: pd.DataFrame) -> list[str]:
    cols = []
    for c in df.columns:
        if "Expiry" in c:
            continue
        if c.endswith("Date") or c.endswith("Month") or c.endswith("TimestampUTC"):
            cols.append(c)
    return cols


def _next_ids(series: pd.Series, n: int) -> list[str]:
    """Continue an ID sequence such as SALE-000500000 -> SALE-000500001."""
    s = series.dropna().astype(str)
    m = s.str.extract(r"^(.*?)(\d+)$")
    nums = m[1].astype(int)
    idx = nums.idxmax()
    prefix, width, start = m.loc[idx, 0], len(m.loc[idx, 1]), nums.max() + 1
    return [f"{prefix}{str(i).zfill(width)}" for i in range(start, start + n)]


def _shift_into_month(df: pd.DataFrame, anchor: str, target: date, rng) -> pd.DataFrame:
    """Move each row so its anchor date lands in the target month (same day-of-month)."""
    df = df.copy()
    anchor_dt = pd.to_datetime(df[anchor].str[:10], errors="coerce")
    days_in_month = pd.Period(target, "M").days_in_month
    day = anchor_dt.dt.day.clip(upper=days_in_month)
    new_anchor = pd.to_datetime(
        dict(year=[target.year] * len(df), month=[target.month] * len(df), day=day.values)
    )
    delta = (new_anchor.values - anchor_dt.dt.normalize().values)
    delta = pd.to_timedelta(delta)
    for c in _date_columns(df):
        is_ts = df[c].str.contains("T", na=False).any()
        dt = pd.to_datetime(df[c], errors="coerce", utc=is_ts)
        shifted = dt + delta.values
        if is_ts:
            df[c] = shifted.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            df[c] = shifted.dt.strftime("%Y-%m-%d")
        df.loc[dt.isna(), c] = np.nan
    return df


def _monthly_volume(df: pd.DataFrame, anchor: str) -> tuple[int, pd.DataFrame]:
    """Median rows per month over the last 12 months + the 12-month row pool."""
    months = pd.to_datetime(df[anchor].str[:10], errors="coerce").dt.to_period("M")
    last = months.max()
    recent_mask = months > (last - 12)
    counts = months[recent_mask].value_counts()
    return int(counts.median()), df[recent_mask]


def _inject_bad_rows(df: pd.DataFrame, share: float, rng, rules: list) -> tuple[pd.DataFrame, int]:
    n_bad = int(round(len(df) * share))
    if n_bad == 0:
        return df, 0
    idx = rng.choice(df.index, size=n_bad, replace=False)
    for i, row_idx in enumerate(idx):
        col, value = rules[i % len(rules)]
        df.loc[row_idx, col] = value
    return df, n_bad


def _num(df, cols):
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


# --------------------------------------------------------------------------- domains
def gen_master(target: date, refresh_id: int, rng) -> tuple[pd.DataFrame, dict]:
    hist = _history("master")
    latest = hist.sort_values("EffectiveFromDate").groupby("MedicineID").tail(1).copy()
    latest = latest.sort_values("MedicineID").reset_index(drop=True)
    changed = rng.choice(latest.index, size=4, replace=False)
    _num(latest, ["WholesalePriceUSD", "RecommendedRetailPriceUSD", "UnitManufacturingCostUSD"])
    for i in changed[:3]:  # price increase -> SCD Type 2 new version
        latest.loc[i, "RecommendedRetailPriceUSD"] = round(latest.loc[i, "RecommendedRetailPriceUSD"] * 1.05, 2)
        latest.loc[i, "WholesalePriceUSD"] = round(latest.loc[i, "WholesalePriceUSD"] * 1.04, 2)
    latest.loc[changed[3], "SafetyStockDays"] = str(int(latest.loc[changed[3], "SafetyStockDays"]) + 7)
    latest.loc[changed, "EffectiveFromDate"] = target.isoformat()
    info = {"changed_medicines": latest.loc[changed, "MedicineID"].tolist()}
    return latest, info


def gen_sales(target: date, refresh_id: int, rng, master: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    hist = _history("sales")
    n, pool = _monthly_volume(hist, "SalesDate")
    month_end = pd.Timestamp(target) + pd.offsets.MonthEnd(0)
    # only sell from lots that will still have >= 30 days of shelf life
    pool = pool[pd.to_datetime(pool["BatchExpiryDate"]) > month_end + pd.Timedelta(days=30)]
    df = pool.sample(n=n, replace=len(pool) < n, random_state=int(rng.integers(1e9))).reset_index(drop=True)
    df = _shift_into_month(df, "SalesDate", target, rng)
    df["SalesTransactionID"] = _next_ids(hist["SalesTransactionID"], n)
    has_rx = df["PrescriptionID"].notna()
    df.loc[has_rx, "PrescriptionID"] = _next_ids(hist["PrescriptionID"], int(has_rx.sum()))
    # one new replenishment delivery per old delivery id (keeps delivery ids consistent)
    old = df["PharmacyDeliveryID"].unique()
    new_ids = _next_ids(hist["PharmacyDeliveryID"], len(old))
    dmap = dict(zip(old, new_ids))
    df["PharmacyDeliveryID"] = df["PharmacyDeliveryID"].map(dmap)
    df["PharmacyOrderID"] = df["PharmacyDeliveryID"].str.replace("PHD-", "PHO-", regex=False)
    # prices follow the latest master (so SCD price changes show up in sales)
    price = master.set_index("MedicineID")["RecommendedRetailPriceUSD"].astype(float)
    _num(df, ["QuantitySoldPacks", "GrossSalesAmountUSD", "DiscountAmountUSD", "SalesTaxUSD",
              "CostOfGoodsSoldUSD", "NetSalesAmountUSD", "ReturnedQuantityPacks"])
    disc_ratio = (df["DiscountAmountUSD"] / df["GrossSalesAmountUSD"]).fillna(0)
    tax_ratio = (df["SalesTaxUSD"] / df["NetSalesAmountUSD"]).fillna(0)
    unit_cost = df["CostOfGoodsSoldUSD"] / df["QuantitySoldPacks"]
    df["UnitRetailPriceUSD"] = df["MedicineID"].map(price).round(2)
    df["GrossSalesAmountUSD"] = (df["QuantitySoldPacks"] * df["UnitRetailPriceUSD"]).round(2)
    df["DiscountAmountUSD"] = (df["GrossSalesAmountUSD"] * disc_ratio).round(2)
    df["NetSalesAmountUSD"] = (df["GrossSalesAmountUSD"] - df["DiscountAmountUSD"]).round(2)
    df["SalesTaxUSD"] = (df["NetSalesAmountUSD"] * tax_ratio).round(2)
    df["CostOfGoodsSoldUSD"] = (df["QuantitySoldPacks"] * unit_cost).round(2)
    df["GrossMarginUSD"] = (df["NetSalesAmountUSD"] - df["CostOfGoodsSoldUSD"]).round(2)
    df["DaysToExpiryAtSale"] = (pd.to_datetime(df["BatchExpiryDate"]) - pd.to_datetime(df["SalesDate"])).dt.days
    df = df.sort_values("SalesTimestampUTC").reset_index(drop=True)
    df, bad = _inject_bad_rows(df, SALES_BAD_ROW_SHARE, rng, [
        ("UnitRetailPriceUSD", 0), ("MedicineID", np.nan), ("SalesDate", "2026-13-45"),
        ("UnitRetailPriceUSD", -3.5)])
    return df, {"rows": n, "malformed_injected": bad}


def gen_hospital(target: date, refresh_id: int, rng) -> tuple[pd.DataFrame, dict]:
    hist = _history("hospital")
    hist["DemandMonth"] = hist["DemandMonth"].str[:10]
    last_month = hist["DemandMonth"].max()
    prev = hist[hist["DemandMonth"] == last_month].set_index(["HospitalID", "MedicineID"])
    ly = (pd.Timestamp(target) - pd.DateOffset(years=1)).strftime("%Y-%m-%d")
    base = hist[hist["DemandMonth"] == ly]
    if base.empty:
        base = hist[hist["DemandMonth"] == last_month]
    df = base.copy().reset_index(drop=True)
    qty = ["EstimatedDiseaseCases", "DiseaseRelatedAdmissions", "OutpatientEncounters", "ForecastDemandPacks",
           "RequestedQuantityPacks", "ApprovedQuantityPacks", "ReceivedQuantityPacks",
           "EmergencyRequestQuantityPacks", "DispensedQuantityPacks", "ConsumedQuantityPacks"]
    _num(df, qty + ["WastageQuantityPacks", "UnfulfilledDemandPacks", "StockoutDays"])
    growth = rng.normal(1.04, 0.07, size=len(df)).clip(0.8, 1.3)  # ~4 % year-on-year growth
    for c in qty:
        df[c] = (df[c] * growth).round().astype(int)
    key = list(zip(df["HospitalID"], df["MedicineID"]))
    df["OpeningStockPacks"] = [int(prev.loc[k, "ClosingStockPacks"]) if k in prev.index else 0 for k in key]
    avail = df["OpeningStockPacks"] + df["ReceivedQuantityPacks"] - df["WastageQuantityPacks"]
    short = (df["DispensedQuantityPacks"] - avail).clip(lower=0)
    df["DispensedQuantityPacks"] = df["DispensedQuantityPacks"] - short
    df["ConsumedQuantityPacks"] = df[["ConsumedQuantityPacks", "DispensedQuantityPacks"]].min(axis=1)
    df["UnfulfilledDemandPacks"] = np.where(short > 0, short, (df["UnfulfilledDemandPacks"] * growth).round()).astype(int)
    df["StockoutDays"] = np.where(short > 0, rng.integers(1, 6, len(df)), df["StockoutDays"]).astype(int)
    df["ClosingStockPacks"] = (avail - df["DispensedQuantityPacks"]).astype(int)
    tot = df["DispensedQuantityPacks"] + df["UnfulfilledDemandPacks"]
    df["ServiceLevelPct"] = np.where(tot > 0, (df["DispensedQuantityPacks"] / tot * 100).round(2), 100.0)
    df["MedicationRequestStatus"] = np.where(df["UnfulfilledDemandPacks"] > 0, "Partially fulfilled", "Completed")
    df["DemandMonth"] = target.isoformat()
    df["HospitalDemandRecordID"] = _next_ids(hist["HospitalDemandRecordID"], len(df))
    df, bad = _inject_bad_rows(df, HOSPITAL_BAD_ROW_SHARE, rng, [
        ("MedicineID", np.nan), ("ForecastDemandPacks", -50), ("DemandMonth", "2026-02-30")])
    return df, {"rows": len(df), "malformed_injected": bad}


def gen_generic(key: str, target: date, refresh_id: int, rng) -> tuple[pd.DataFrame, dict]:
    """Time-shifted bootstrap for manufacturing, deliveries, warehouse snapshots and IoT."""
    fname, _tbl, pk, anchor = SOURCES[key]
    hist = _history(key)
    n, pool = _monthly_volume(hist, anchor)
    df = pool.sample(n=n, replace=len(pool) < n, random_state=int(rng.integers(1e9))).reset_index(drop=True)
    df = _shift_into_month(df, anchor, target, rng)
    df[pk[0]] = _next_ids(hist[pk[0]], n)
    if key == "manufacturing":
        df["ProductionBatchID"] = [f"B{target:%y}-{p}-R{refresh_id}{i:05d}" for i, p in enumerate(df["PlantID"])]
        df["ExpiryDate"] = (pd.to_datetime(df["ActualCompletionDate"]) + pd.DateOffset(months=24)).dt.strftime("%Y-%m-%d")
    if key == "warehouse":
        df["DaysToExpiry"] = (pd.to_datetime(df["BatchExpiryDate"]) - pd.to_datetime(df["SnapshotDate"])).dt.days
    if key == "iot":
        df = df.sort_values("EventTimestampUTC").reset_index(drop=True)
    df, bad = _inject_bad_rows(df, OTHER_BAD_ROW_SHARE, rng, [(pk[0], np.nan)])
    return df, {"rows": n, "malformed_injected": bad}


# --------------------------------------------------------------------------- driver
def next_target_month() -> date:
    sales = _history("sales")
    last = pd.to_datetime(sales["SalesDate"].str[:10], errors="coerce").max()
    nxt = (last + pd.offsets.MonthBegin(1)).date()
    return nxt


def generate(refresh_id: int, target: date | None = None, seed: int | None = None) -> dict:
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    target = target or next_target_month()
    rng = np.random.default_rng(seed if seed is not None else 1000 + refresh_id)
    tag = f"__r{refresh_id}_{target:%Y%m}"
    stats = {"refresh_id": refresh_id, "target_month": target.isoformat(), "files": {}}

    master, minfo = gen_master(target, refresh_id, rng)
    outputs = {"master": (master, minfo)}
    outputs["sales"] = gen_sales(target, refresh_id, rng, master)
    outputs["hospital"] = gen_hospital(target, refresh_id, rng)
    for key in ("manufacturing", "delivery", "warehouse", "iot"):
        outputs[key] = gen_generic(key, target, refresh_id, rng)

    for key, (df, info) in outputs.items():
        fname = SOURCES[key][0][:-4] + tag + ".csv"
        df.to_csv(GENERATED_DIR / fname, index=False)
        # sales are POS events -> replayed through the streaming path; everything else is a batch file
        dest = STREAM_DIR if key == "sales" else LANDING_DIR
        df.to_csv(dest / fname, index=False)
        stats["files"][key] = {"file": fname, "rows": len(df), "columns": df.shape[1],
                               "path": "stream" if key == "sales" else "batch", **info}

    all_stats = json.loads(STATS_FILE.read_text()) if STATS_FILE.exists() else []
    all_stats = [s for s in all_stats if s["refresh_id"] != refresh_id] + [stats]
    STATS_FILE.write_text(json.dumps(all_stats, indent=2, default=str))
    return stats


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Generate one refresh worth of synthetic data")
    ap.add_argument("--refresh-id", type=int, required=True)
    ap.add_argument("--month", help="YYYY-MM (default: month after the latest sales date)")
    ap.add_argument("--seed", type=int)
    a = ap.parse_args()
    tgt = date.fromisoformat(a.month + "-01") if a.month else None
    print(json.dumps(generate(a.refresh_id, tgt, a.seed), indent=2, default=str))
