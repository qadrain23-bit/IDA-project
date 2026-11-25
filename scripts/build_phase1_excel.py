"""Builds docs/Phase1_SDI_Data_Mart.xlsx - the Phase 1 Excel deliverable.

ASSUMPTION: the project plan lists "Excel file Phase 1 (2.5%)" without saying what it must
contain. This workbook documents the mart design and its real outputs:
bus matrix, data dictionary, source-to-target mapping, validation rules, refresh log,
KPI query results and query-performance figures. Everything comes from the live run.
"""
import json
import sys
from pathlib import Path

import duckdb
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from pipeline.config import DUCKDB_PATH  # noqa: E402
from pipeline.queries import QUERIES, results  # noqa: E402

FONT = "Arial"
HDR_FILL = PatternFill("solid", fgColor="1C5CAB")
SUB_FILL = PatternFill("solid", fgColor="E8F0FB")
THIN = Side(style="thin", color="C8C8C8")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)

wb = Workbook()
# Monitoring numbers come from the real run on the team laptop (docs/laptop_run_results.json);
# KPI results are read from the DuckDB mart (same data on every machine: the generator is seeded).
RUN = json.loads((ROOT / "docs" / "laptop_run_results.json").read_text())
duck = duckdb.connect(str(DUCKDB_PATH), read_only=True)


def sheet(title, header, rows, widths=None, note=None):
    ws = wb.create_sheet(title)
    r0 = 1
    if note:
        ws.cell(1, 1, note).font = Font(name=FONT, italic=True, size=9, color="555555")
        ws.cell(1, 1).alignment = Alignment(wrap_text=True, vertical="top")
        ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=max(len(header), 4))
        ws.row_dimensions[1].height = 30
        r0 = 3
    for j, h in enumerate(header, 1):
        c = ws.cell(r0, j, h)
        c.font = Font(name=FONT, bold=True, color="FFFFFF", size=10)
        c.fill = HDR_FILL
        c.alignment = Alignment(wrap_text=True, vertical="center")
        c.border = BORDER
    for i, row in enumerate(rows, r0 + 1):
        for j, v in enumerate(row, 1):
            c = ws.cell(i, j, v)
            c.font = Font(name=FONT, size=9)
            c.border = BORDER
            c.alignment = Alignment(wrap_text=True, vertical="top")
    for j in range(1, len(header) + 1):
        ws.column_dimensions[get_column_letter(j)].width = (widths or {}).get(j, 18)
    ws.freeze_panes = ws.cell(r0 + 1, 1)
    return ws, r0


# ------------------------------------------------------------------ 1. About
ws = wb.active
ws.title = "About"
lines = [
    ("Phase 1 - Sales & Demand Intelligence (SDI) Data Mart", True, 14),
    ("Intelligent Data Architectures, Fall 2026 - Pharma Cold Chain DWH + BI pipeline", False, 10),
    ("Group members: Qadrain Qadir, Muhammad Alam", False, 10),
    ("", False, 10),
    ("What this workbook contains (one tab each):", True, 10),
    ("Bus Matrix - which dimensions each fact table uses (conformed dimensions for the later fact constellation)", False, 10),
    ("Data Dictionary - every star-schema table and column, its type, meaning and SCD handling", False, 10),
    ("Source-to-Target - which source CSV column feeds which mart column, and the rule applied", False, 10),
    ("Validation Rules - checks at ingestion and how many rows each rule rejected in the real run", False, 10),
    ("Refresh Log - rows generated / loaded / rejected for the initial load and the 2 refreshes", False, 10),
    ("KPI Results - output of the Phase 1 analytical queries after refresh 2 (feeds the Milestone 2 BI dashboard)", False, 10),
    ("Query Performance - average time of each query, star-schema version vs optimized version", False, 10),
    ("", False, 10),
    ("Assumption: the project plan does not define the content of 'Excel file Phase 1'; this is our interpretation.", False, 9),
    ("Monitoring numbers (loads, rejects, query times) are from the real run on the team laptop (Docker Desktop, 26 Nov 2026);", False, 9),
    ("KPI results are read from the DuckDB mart. Nothing is typed by hand.", False, 9),
]
for i, (t, b, s) in enumerate(lines, 1):
    ws.cell(i, 1, t).font = Font(name=FONT, bold=b, size=s)
ws.column_dimensions["A"].width = 110

# ------------------------------------------------------------------ 2. Bus matrix
dims = ["dim_date (sale)", "dim_date (delivery)", "dim_date (expiry)", "dim_date (month)", "dim_medicine (SCD2)",
        "dim_pharmacy", "dim_hospital", "dim_sales_profile (junk)", "dim_demand_status (junk)"]
bus = [
    ["fact_sales", "Transaction fact", "1 row = 1 sales transaction", "X", "X", "X", "", "X", "X", "X", "X", ""],
    ["fact_hospital_demand", "Periodic snapshot (monthly)", "1 row = 1 hospital x medicine x month", "", "", "", "X", "X", "", "X", "", "X"],
    ["agg_sales_monthly", "Aggregate (summary) fact", "1 row = month x medicine x channel group", "X (month)", "", "", "", "X", "", "", "X", ""],
    ["agg_demand_monthly", "Aggregate (summary) fact", "1 row = month x hospital x therapeutic class", "", "", "", "X", "X", "", "X", "", ""],
]
sheet("Bus Matrix", ["Fact table", "Fact type", "Grain"] + dims, bus, {1: 22, 2: 22, 3: 34},
      note="X = the fact uses this dimension. dim_date is one table used in 4 roles (role-playing dimension). "
           "dim_medicine, dim_hospital and dim_date are conformed: Phase 2 and 3 marts reuse them.")

# ------------------------------------------------------------------ 3. Data dictionary
desc = {
    "date_key": "Surrogate key yyyymmdd", "medicine_key": "Surrogate key (one per medicine version)",
    "medicine_id": "Business key from source (MedicineID)", "valid_from": "SCD2: first day this version is valid",
    "valid_to": "SCD2: last day valid (9999-12-31 = current)", "is_current": "SCD2: TRUE for the current version",
    "version_no": "SCD2: version counter", "scd_hash": "MD5 of Type-2 attributes, used to detect changes",
    "channel_group": "Derived: Retail / Hospital / E-commerce", "bed_size_band": "Derived from licensed beds",
    "profile_hash": "MD5 of all junk attributes (lookup key)", "status_hash": "MD5 of junk attributes (lookup key)",
    "sales_transaction_id": "Degenerate dimension (source transaction id)", "load_id": "Lineage: staging load that delivered the row",
    "opening_stock_packs": "Semi-additive: do not sum across months", "closing_stock_packs": "Semi-additive: do not sum across months",
    "service_level_pct": "Non-additive: average, never sum",
}
type2 = {"unit_mfg_cost_usd", "wholesale_price_usd", "retail_price_usd", "safety_stock_days", "product_status"}
dd = []
for t in ["dim_date", "dim_medicine", "dim_pharmacy", "dim_hospital", "dim_sales_profile", "dim_demand_status",
          "fact_sales", "fact_hospital_demand", "agg_sales_monthly", "agg_demand_monthly"]:
    for name, typ, *_ in duck.execute(f"DESCRIBE {t}").fetchall():
        scd = ""
        if t == "dim_medicine":
            scd = "Type 2 (history kept)" if name in type2 else ("SCD housekeeping" if name in ("valid_from", "valid_to", "is_current", "version_no", "scd_hash") else "Type 1 (overwrite)")
        elif t in ("dim_pharmacy", "dim_hospital"):
            scd = "Type 1 (overwrite)"
        dd.append([t, name, typ, scd, desc.get(name, name.replace("_", " "))])
sheet("Data Dictionary", ["Table", "Column", "DuckDB type", "SCD handling", "Meaning"], dd, {1: 22, 2: 28, 3: 16, 4: 20, 5: 55})

# ------------------------------------------------------------------ 4. Source to target
stt = [
    ["01_master_data.csv", "MedicineID, names, class, form, storage", "dim_medicine", "same names (snake_case)", "Type 1 overwrite"],
    ["01_master_data.csv", "UnitManufacturingCostUSD, WholesalePriceUSD, RecommendedRetailPriceUSD, SafetyStockDays, ProductStatus", "dim_medicine", "Type-2 columns", "Hash compare; change -> close old row (valid_to = new date - 1), insert new version"],
    ["01_master_data.csv", "EffectiveFromDate", "dim_medicine.valid_from", "valid_from", "first version valid from 1900-01-01"],
    ["05_pharmacy_sales_data.csv", "SalesTransactionID", "fact_sales", "sales_transaction_id", "degenerate dimension; INSERT OR IGNORE (idempotent)"],
    ["05_pharmacy_sales_data.csv", "SalesDate / PharmacyDeliveryDate / BatchExpiryDate", "fact_sales", "sales_date_key / delivery_date_key / expiry_date_key", "date -> yyyymmdd role-playing keys"],
    ["05_pharmacy_sales_data.csv", "MedicineID + SalesDate", "fact_sales", "medicine_key", "SCD2 lookup: version where SalesDate BETWEEN valid_from AND valid_to"],
    ["05_pharmacy_sales_data.csv", "PharmacyID, PharmacyName, PharmacyType, City, State, HospitalID", "dim_pharmacy", "pharmacy_key", "Type 1; unknown -> -1"],
    ["05_pharmacy_sales_data.csv", "HospitalID", "fact_sales", "hospital_key", "lookup dim_hospital; not a hospital pharmacy -> -1"],
    ["05_pharmacy_sales_data.csv", "SalesChannel, PharmacyType, CustomerSegment, PayerType, PaymentMethod, PrescriptionRequiredFlag, PromotionCode, EcommerceDeliveryStatus, ReturnFlag, SaleStatus", "dim_sales_profile", "sales_profile_key", "junk dimension: one key per combination; channel_group derived; null promo -> 'NONE'"],
    ["05_pharmacy_sales_data.csv", "QuantitySoldPacks ... GrossMarginUSD, ReturnedQuantityPacks, DaysToExpiryAtSale", "fact_sales", "measures", "copied after validation (price > 0, qty > 0)"],
    ["05_pharmacy_sales_data.csv", "PrescriptionID, PharmacyDeliveryID, WarehouseLotID, ProductionBatchID", "fact_sales", "degenerate columns", "kept for traceability to Phase 2/3"],
    ["06_hospital_demand_data.csv", "HospitalID + hospital attributes", "dim_hospital", "hospital_key", "Type 1; bed_size_band derived"],
    ["06_hospital_demand_data.csv", "DemandMonth", "fact_hospital_demand", "month_date_key", "first day of month -> yyyymmdd"],
    ["06_hospital_demand_data.csv", "MedicationRequestStatus, FormularyStatus", "dim_demand_status", "demand_status_key", "junk dimension"],
    ["06_hospital_demand_data.csv", "ForecastDemandPacks ... ServiceLevelPct", "fact_hospital_demand", "measures", "copied after validation (no negative quantities)"],
]
sheet("Source-to-Target", ["Source file", "Source column(s)", "Target table", "Target column(s)", "Transformation rule"], stt,
      {1: 26, 2: 48, 3: 20, 4: 30, 5: 55})

# ------------------------------------------------------------------ 5. Validation rules
rej = {}
for r in RUN["rejected_rows"]:
    rej[(r["source_key"], r["reason"])] = rej.get((r["source_key"], r["reason"]), 0) + r["n"]
rules = [
    ("all 7 files", "Primary key not null", "null primary key"),
    ("all 7 files", "Every date / timestamp parses (YYYY-MM-DD)", "invalid date"),
    ("all 7 files", "Numeric columns are numbers; whole-number columns have no decimals", "non-numeric"),
    ("sales", "Non-null SKU (MedicineID)", "null MedicineID (SKU)"),
    ("sales", "Unit price > 0", "price <= 0"),
    ("sales", "Quantity sold > 0", "quantity <= 0"),
    ("sales", "MedicineID exists in the medicine master", "unknown MedicineID"),
    ("hospital", "Non-null MedicineID and HospitalID", "null MedicineID (SKU)"),
    ("hospital", "No negative pack quantities", "negative quantity"),
    ("master", "Retail price > 0 and min temp <= max temp", "retail price <= 0"),
]
vrows = []
for scope, rule, match in rules:
    n = sum(c for (src, reason), c in rej.items()
            if reason.startswith(match) and (scope == "all 7 files" or src == scope))
    vrows.append([scope, rule, match, n])
ws, r0 = sheet("Validation Rules", ["Applies to", "Rule", "Reason text stored in staging.rejected_rows", "Rows rejected (real run)"],
               vrows, {1: 14, 2: 55, 3: 36, 4: 16},
               note="Rejected rows are not loaded; they are kept with the reason in staging.rejected_rows. Counts are from the laptop run (initial load + 2 refreshes).")
last = r0 + len(vrows)
ws.cell(last + 1, 3, "Total rejected").font = Font(name=FONT, bold=True, size=9)
ws.cell(last + 1, 4, f"=SUM(D{r0 + 1}:D{last})").font = Font(name=FONT, bold=True, size=9)

# ------------------------------------------------------------------ 6. Refresh log
log = [[b["refresh_id"], b["source_key"], b["status"], b["rows_read"] or 0, b["rows_loaded"] or 0, b["rows_rejected"] or 0,
        b["duration_s"], "batch"] for b in RUN["batch_loads"]]
log += [[s_["refresh_id"], "sales", "SUCCESS", s_["events_sent"], s_["events_loaded"], s_["events_malformed"],
         s_["duration_s"], "stream"] for s_ in RUN["stream_runs"]]
log.sort(key=lambda r: (r[0], r[7], r[1]))
ws, r0 = sheet("Refresh Log", ["Refresh", "Source", "Status", "Rows read", "Rows loaded", "Rows rejected", "Load time (s)",
                               "Path", "Reject rate"], log, {1: 9, 2: 14, 3: 12, 4: 12, 5: 12, 6: 12, 7: 12, 8: 9, 9: 11},
               note="Laptop run. Refresh 0 = initial history (2023-2025), 1 = Jan 2026, 2 = Feb 2026. The 3 FAILED hospital "
                    "loads in refresh 0 came from a pandas-version bug that was then fixed; the file was re-loaded.")
for i in range(r0 + 1, r0 + 1 + len(log)):
    c = ws.cell(i, 9, f"=IF(D{i}=0,0,F{i}/D{i})")
    c.number_format = "0.00%"
    c.font = Font(name=FONT, size=9)
    c.border = BORDER
    ws.cell(i, 4).number_format = ws.cell(i, 5).number_format = ws.cell(i, 6).number_format = "#,##0"
end = r0 + len(log)
tot_row = end + 2
ws.cell(tot_row, 1, "Totals by refresh").font = Font(name=FONT, bold=True, size=10)
ws.cell(tot_row + 1, 1, "Refresh").font = Font(name=FONT, bold=True, size=9)
for j, h in enumerate(["Rows read", "Rows loaded", "Rows rejected"], 4):
    ws.cell(tot_row + 1, j, h).font = Font(name=FONT, bold=True, size=9)
for k in range(3):
    rr = tot_row + 2 + k
    ws.cell(rr, 1, k).font = Font(name=FONT, size=9)
    for j, col in zip((4, 5, 6), ("D", "E", "F")):
        c = ws.cell(rr, j, f"=SUMIFS({col}{r0 + 1}:{col}{end},$A{r0 + 1}:$A{end},$A{rr})")
        c.number_format = "#,##0"
        c.font = Font(name=FONT, size=9)

# ------------------------------------------------------------------ 7. KPI results
ws = wb.create_sheet("KPI Results")
row = 1
ws.cell(row, 1, "Phase 1 analytical query results after refresh 2 (optimized queries; identical to the star-schema versions)").font = Font(name=FONT, bold=True, size=11)
row += 2
for widget in ["channel_revenue_margin", "top10_fast_moving", "bottom10_slow_moving_cold_chain", "unfulfilled_demand_by_hospital", "kpi_cards"]:
    cols, rows = results(widget)
    ws.cell(row, 1, f"{widget} - {QUERIES[widget][0]}").font = Font(name=FONT, bold=True, size=10)
    row += 1
    for j, c in enumerate(cols, 1):
        cell = ws.cell(row, j, c)
        cell.font = Font(name=FONT, bold=True, size=9)
        cell.fill = SUB_FILL
        cell.border = BORDER
    for r in rows:
        row += 1
        for j, v in enumerate(r, 1):
            v = float(v) if hasattr(v, "as_tuple") else v
            cell = ws.cell(row, j, v)
            cell.font = Font(name=FONT, size=9)
            cell.border = BORDER
            if isinstance(v, float):
                cell.number_format = "#,##0.00"
    row += 3
for j, w in enumerate([30, 32, 16, 16, 16, 16], 1):
    ws.column_dimensions[get_column_letter(j)].width = w

# forecast vs actual summary by year (from the summary table)
fva = duck.execute("""SELECT substr(year_month, 1, 4) yr, sum(forecast_packs), sum(consumed_packs), sum(unfulfilled_packs)
                      FROM agg_demand_monthly GROUP BY 1 ORDER BY 1""").fetchall()
ws.cell(row, 1, "forecast_vs_actual (yearly roll-up) - hospital forecast vs actual consumption").font = Font(name=FONT, bold=True, size=10)
row += 1
for j, h in enumerate(["Year", "Forecast packs", "Consumed packs", "Unfulfilled packs", "Actual / forecast"], 1):
    c = ws.cell(row, j, h)
    c.font = Font(name=FONT, bold=True, size=9)
    c.fill = SUB_FILL
    c.border = BORDER
for yr, f, cns, u in fva:
    row += 1
    for j, v in enumerate([yr, int(f), int(cns), int(u)], 1):
        c = ws.cell(row, j, v)
        c.font = Font(name=FONT, size=9)
        c.border = BORDER
        if j > 1:
            c.number_format = "#,##0"
    c = ws.cell(row, 5, f"=IF(B{row}=0,0,C{row}/B{row})")
    c.number_format = "0.000"
    c.font = Font(name=FONT, size=9)
    c.border = BORDER
ws.cell(row + 1, 1, "2026 = January and February only (the two generated refresh months).").font = Font(name=FONT, italic=True, size=8)

# ------------------------------------------------------------------ 8. Query performance
qp = [[w["widget"], w["base_ms"], w["optimized_ms"], w["runs"]] for w in RUN["query_performance"]["per_widget"]]
ws, r0 = sheet("Query Performance", ["Dashboard widget / query", "Avg ms - star schema joins", "Avg ms - optimized",
                                     "Runs logged", "Speed-up (x)", "Optimization used"],
               [r + [None, None] for r in qp], {1: 34, 2: 16, 3: 16, 4: 11, 5: 12, 6: 60},
               note="Laptop run: each query 5 times per query-layer run, 4 runs, 0 failures. Optimized = summary tables, date-sorted "
                    "fact (zone maps), ART indexes, or year/month parquet partitions. Speed-up = star-schema time / optimized time; "
                    "below 1 means the optimization made it slower on this machine.")
how = {"latest_month_partition_scan": "parquet partition read - much slower here: reading parquet from a Windows folder mounted into Docker is slow",
       "unfulfilled_demand_by_hospital": "agg_demand_monthly (fact is already small: 28k rows)"}
for i in range(r0 + 1, r0 + 1 + len(qp)):
    c = ws.cell(i, 5, f"=IF(C{i}=0,0,B{i}/C{i})")
    c.number_format = "0.0\"x\""
    c.font = Font(name=FONT, size=9)
    c.border = BORDER
    w = ws.cell(i, 1).value
    c = ws.cell(i, 6, how.get(w, "agg_sales_monthly / agg_demand_monthly summary table"))
    c.font = Font(name=FONT, size=9)
    c.border = BORDER

out = ROOT / "docs" / "Phase1_SDI_Data_Mart.xlsx"
wb.save(out)
print("wrote", out)
