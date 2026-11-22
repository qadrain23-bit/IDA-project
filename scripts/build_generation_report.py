"""Builds docs/Data_Generation_Report.pdf (1-2 pages) from the real generator stats
(data/generated/generation_stats.json) and the source files' monthly volumes."""
import json
import sys
from pathlib import Path

import duckdb
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from pipeline.config import SOURCE_DIR, SOURCES  # noqa: E402

stats = {s["refresh_id"]: s for s in json.loads((ROOT / "data/generated/generation_stats.json").read_text())}
r1, r2 = stats[1], stats[2]

ss = getSampleStyleSheet()
body = ParagraphStyle("b", parent=ss["Normal"], fontName="Helvetica", fontSize=9.5, leading=12.5, spaceAfter=4)
h1 = ParagraphStyle("h1", parent=ss["Title"], fontSize=15, leading=18, spaceAfter=2, alignment=0)
h2 = ParagraphStyle("h2", parent=ss["Heading2"], fontSize=11, leading=13, spaceBefore=7, spaceAfter=3,
                    textColor=colors.HexColor("#1c5cab"))
small = ParagraphStyle("s", parent=body, fontSize=8.5, leading=11, textColor=colors.HexColor("#555555"))
cell = ParagraphStyle("c", parent=body, fontSize=8, leading=10, spaceAfter=0)


def tbl(rows, widths):
    t = Table([[Paragraph(str(c), cell) for c in r] for r in rows], colWidths=widths, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e8f0fb")),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#c8c8c8")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 2), ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    return t


# monthly volume evidence from the original files
con = duckdb.connect()
vol_rows = [["Source file", "Rows (original)", "Months", "Mean rows / month (all)", "Median rows / month (last 12 m)",
             "Refresh 1 (Jan 2026)", "Refresh 2 (Feb 2026)", "Path"]]
labels = {"master": "01 Master", "manufacturing": "02 Manufacturing", "delivery": "03 Deliveries",
          "warehouse": "04 Warehouse snapshots", "sales": "05 Pharmacy sales", "hospital": "06 Hospital demand",
          "iot": "07 IoT readings"}
tot1 = tot2 = tot0 = 0
for key, (fname, _t, _pk, anchor) in SOURCES.items():
    n = con.execute(f"SELECT count(*) FROM read_csv('{SOURCE_DIR / fname}')").fetchone()[0]
    tot0 += n
    a, b = r1["files"][key]["rows"], r2["files"][key]["rows"]
    tot1, tot2 = tot1 + a, tot2 + b
    if key == "master":
        vol_rows.append([labels[key], n, "-", "-", "-", f"{a} (4 changed)", f"{b} (4 changed)", "batch"])
        continue
    months, mean, med = con.execute(f"""
        WITH m AS (SELECT strftime("{anchor}"::DATE, '%Y-%m') ym, count(*) n FROM read_csv('{SOURCE_DIR / fname}') GROUP BY 1),
             l AS (SELECT * FROM m ORDER BY ym DESC LIMIT 12)
        SELECT (SELECT count(*) FROM m), (SELECT avg(n) FROM m), (SELECT median(n) FROM l)""").fetchone()
    vol_rows.append([labels[key], f"{n:,}", months, f"{mean:,.0f}", f"{med:,.0f}", f"{a:,}", f"{b:,}",
                     "stream" if key == "sales" else "batch"])
vol_rows.append(["<b>Total</b>", f"<b>{tot0:,}</b>", "", "", "", f"<b>{tot1:,}</b>", f"<b>{tot2:,}</b>", ""])
jan25 = con.execute(f"""SELECT count(*) FROM read_csv('{SOURCE_DIR / SOURCES['sales'][0]}')
                         WHERE strftime(SalesDate::DATE, '%Y-%m') = '2025-01'""").fetchone()[0]

bad1 = sum(v.get("malformed_injected", 0) or 0 for v in r1["files"].values())
bad2 = sum(v.get("malformed_injected", 0) or 0 for v in r2["files"].values())

story = [
    Paragraph("Data Generation (Oversampling) Methodology - Phase 1, Sales &amp; Demand Intelligence", h1),
    Paragraph("Intelligent Data Architectures, Fall 2026 - Pharmaceutical Cold Supply Chain DWH + BI pipeline. "
              "Group members: ____________________", small),
    Paragraph("1. Goal", h2),
    Paragraph("The warehouse must be seen <b>refreshing twice</b> with new data while keeping all history. The generator "
              "(<font face='Courier'>src/pipeline/generator.py</font>, run by the Airflow DAG "
              "<font face='Courier'>sdi_00_generate_data</font>) creates <b>one new calendar month</b> per refresh for "
              "<b>all 7 source files</b>: refresh 1 = January 2026, refresh 2 = February 2026 (the original data ends on "
              "31 Dec 2025). New rows are <b>added</b> to the existing 2023-2025 data; nothing is deleted.", body),
    Paragraph("2. How much data, and why", h2),
    Paragraph("<b>Rule: rows per refresh = median rows per month over the latest 12 months of history.</b> One month per "
              "refresh keeps the new data on the same time grain as the old data, so trends stay realistic. We used "
              f"the <b>median</b>, not the mean, because the sales file has a big outlier: January 2025 alone has {jan25:,} "
              "sales (about 7 times a normal month). The mean (13,889 a month) would have made each refresh about twice as "
              "large as recent normal months. Hospital demand is not sampled: it always has exactly one row for each of the "
              "12 hospitals x 63 medicines = 756 rows per month. The master file is re-sent in full (63 SKUs) with 4 "
              "changes, to test Slowly Changing Dimensions.", body),
    tbl(vol_rows, [3.0 * cm, 1.9 * cm, 1.55 * cm, 2.2 * cm, 2.45 * cm, 2.15 * cm, 2.15 * cm, 1.3 * cm]),
    Spacer(1, 3),
    Paragraph(f"Each refresh adds about {tot1:,} rows (about {100 * tot1 / tot0:.1f}% of the original {tot0:,}). This is enough "
              "to show visible movement on the dashboards (a new month on every trend line, new SCD versions, changed "
              "totals) while the load still finishes in seconds, so the refresh can be shown live in the demo.", body),
    Paragraph("3. How the rows are made (time-shifted bootstrap)", h2),
    Paragraph("&bull; <b>Sampling:</b> real rows from the last 12 months are randomly sampled (fixed random seed per refresh, "
              "so runs can be repeated). This keeps the real mix of medicines, pharmacies, channels, payers and "
              "hospitals.<br/>"
              "&bull; <b>Time shift:</b> each sampled row is moved into the target month on the same day of the month. "
              "<i>All</i> date columns in the row are moved by the same number of days, so gaps such as order-to-delivery "
              "or event-to-ingestion stay realistic. Batch expiry dates are <b>not</b> moved because expiry belongs to the "
              "physical batch; sales rows are only drawn from lots with at least 30 days of shelf life left.<br/>"
              "&bull; <b>New keys:</b> primary keys continue the old sequence (e.g. SALE-000500001 onwards); prescription and "
              "pharmacy-delivery IDs are also re-numbered consistently.<br/>"
              "&bull; <b>Recomputed fields:</b> sales prices come from the latest master price; gross = qty x price, "
              "net = gross - discount, margin = net - COGS, days-to-expiry recalculated. Hospital demand gets about 4% "
              "year-on-year growth (random 0.8x-1.3x), opening stock = last month's closing stock, and closing = opening + "
              "received - dispensed - wastage (shortages become unfulfilled demand and stockout days).<br/>"
              "&bull; <b>Master changes (SCD Type 2 test):</b> per refresh, 3 medicines get +5% retail and +4% wholesale "
              f"price and 1 gets +7 safety-stock days (refresh 1: {', '.join(r1['files']['master']['changed_medicines'])}; "
              f"refresh 2: {', '.join(r2['files']['master']['changed_medicines'])}).", body),
    Paragraph("4. Deliberately broken rows (to test validation)", h2),
    Paragraph(f"About 0.4% of sales events, 0.3% of hospital rows and 0.2% of other rows are corrupted on purpose: price set to "
              f"0 or negative, missing MedicineID, impossible date (2026-13-45), negative quantity, missing primary key. "
              f"Injected: {bad1} rows in refresh 1 and {bad2} in refresh 2. In the real run the validation step rejected "
              f"exactly these rows (they are stored in <font face='Courier'>staging.rejected_rows</font> with the reason) "
              f"and they show up on the ingestion dashboards.", body),
    Paragraph("5. Where the data goes", h2),
    Paragraph("Sales (POS transactions) go to <font face='Courier'>data/stream_in/</font> and are replayed as a live event "
              "stream (micro-batches of 50 events, about 400-600 events/second) through the ingestion API. The other six "
              "files go to <font face='Courier'>data/landing/</font>, where the file-watcher picks them up for batch "
              "ingestion. A copy of everything generated is kept in <font face='Courier'>data/generated/</font> with "
              "<font face='Courier'>generation_stats.json</font>.", body),
    Paragraph("6. Assumptions and limits", h2),
    Paragraph("&bull; The next months behave like the last 12 months (no new medicines, pharmacies or hospitals).<br/>"
              "&bull; Manufacturing, delivery, warehouse and IoT rows are generated now but only used by the Phase 2/3 "
              "marts; in Phase 1 they are ingested and stored in staging only.<br/>"
              "&bull; Bootstrapped rows reuse existing lot and batch IDs in sales, so lot-level stock is not re-balanced; "
              "this is fine for sales/demand analysis but Phase 2 should regenerate warehouse snapshots from stock flows.", body),
]
out = ROOT / "docs" / "Data_Generation_Report.pdf"
SimpleDocTemplate(str(out), pagesize=A4, leftMargin=1.6 * cm, rightMargin=1.6 * cm, topMargin=1.4 * cm,
                  bottomMargin=1.4 * cm, title="Data Generation Methodology - Phase 1 SDI").build(story)
print("wrote", out)
