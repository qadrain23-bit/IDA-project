"""Transformation & dimensional-modeling layer: Postgres staging -> DuckDB SDI mart.

Incremental: only staging rows with _load_id above the stored watermark are
processed, so every refresh ADDS to the warehouse (history is never wiped).

Order of work
  1. extract new staging rows (master, sales, hospital demand)
  2. dimensions: dim_medicine (SCD Type 2), dim_pharmacy & dim_hospital (Type 1),
     junk dimensions dim_sales_profile & dim_demand_status
  3. facts: fact_sales (transaction) and fact_hospital_demand (periodic snapshot),
     surrogate-key lookups, SCD2 lookup by sale date, unknown member -1
  4. summary tables + parquet export partitioned by year/month (gold layer)
  5. move the watermark forward
"""
from __future__ import annotations

import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from .config import SOURCES, WAREHOUSE_DIR
from .db import duck_conn, pg_conn

SQL_DIR = Path(__file__).parent / "sql"
PHASE1 = ("master", "sales", "hospital")
TYPE2_COLS = ["unit_mfg_cost_usd", "wholesale_price_usd", "retail_price_usd", "safety_stock_days", "product_status"]


# ----------------------------------------------------------------------------- setup
def init_mart(con):
    con.execute((SQL_DIR / "10_mart_ddl.sql").read_text())
    if con.execute("SELECT count(*) FROM dim_date").fetchone()[0] == 0:
        con.execute("""
            INSERT INTO dim_date
            SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER), d::DATE, day(d), isodow(d), dayname(d),
                   isodow(d) >= 6, weekofyear(d), month(d), monthname(d), quarter(d), year(d), strftime(d, '%Y-%m')
            FROM range(DATE '2020-01-01', DATE '2030-12-31', INTERVAL 1 DAY) t(d)""")
    # unknown members (key -1) so a missing reference never drops a fact row
    con.execute("INSERT OR IGNORE INTO dim_pharmacy (pharmacy_key, pharmacy_id, pharmacy_name) VALUES (-1,'UNKNOWN','Unknown')")
    con.execute("INSERT OR IGNORE INTO dim_hospital (hospital_key, hospital_id, hospital_name) VALUES (-1,'N/A','Not a hospital')")


def _watermarks() -> dict[str, int]:
    with pg_conn() as conn, conn.cursor() as cur:
        for k in PHASE1:
            cur.execute("INSERT INTO ops.etl_watermark (source_key) VALUES (%s) ON CONFLICT DO NOTHING", (k,))
        cur.execute("SELECT source_key, last_load_id FROM ops.etl_watermark")
        return dict(cur.fetchall())


def _extract(con, key: str, since: int, tmp: Path) -> tuple[int, int]:
    """COPY new staging rows to a temp CSV and load it into a DuckDB temp table new_<key>."""
    table = SOURCES[key][1]
    out = tmp / f"{key}.csv"
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT coalesce(max(_load_id), %s) FROM staging.{table} WHERE _load_id > %s", (since, since))
        max_id = cur.fetchone()[0]
        with out.open("w") as fh:
            cur.copy_expert(f"COPY (SELECT * FROM staging.{table} WHERE _load_id > {int(since)} "
                            f"AND _load_id <= {int(max_id)}) TO STDOUT WITH CSV HEADER", fh)
    text_cols = {"master": ["GTIN14"], "sales": ["GTIN14", "PharmacyGLN", "HospitalID", "PromotionCode", "PrescriptionID"],
                 "hospital": ["GTIN14", "HospitalGLN", "TraumaLevel"]}[key]
    types = ", ".join(f"'{c}': 'VARCHAR'" for c in text_cols)
    con.execute(f"CREATE OR REPLACE TEMP TABLE new_{key} AS SELECT * FROM read_csv('{out}', header=true, types={{{types}}})")
    n = con.execute(f"SELECT count(*) FROM new_{key}").fetchone()[0]
    return n, max_id


# ----------------------------------------------------------------------------- dimensions
def load_dim_medicine(con) -> dict:
    """SCD Type 2: a change in price / cost / safety stock / status creates a new version."""
    con.execute("""
        CREATE OR REPLACE TEMP TABLE src_med AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT "MedicineID" medicine_id, "GTIN14" gtin14, "GenericName" generic_name,
                   "FictionalBrandName" brand_name, "DiseaseCategory" disease_category,
                   "ICD10Category" icd10_category, "ATCCode" atc_code, "TherapeuticClass" therapeutic_class,
                   "DosageForm" dosage_form, "Strength" strength, "RouteOfAdministration" route,
                   "PackSize" pack_size, "PrescriptionClass" prescription_class,
                   "ControlledMedicineFlag" controlled_flag, "ColdChainFlag" cold_chain_flag,
                   "StorageMinTempC" storage_min_temp_c, "StorageMaxTempC" storage_max_temp_c,
                   "ShelfLifeMonths" shelf_life_months, "UnitManufacturingCostUSD" unit_mfg_cost_usd,
                   "WholesalePriceUSD" wholesale_price_usd, "RecommendedRetailPriceUSD" retail_price_usd,
                   "SafetyStockDays" safety_stock_days, "ProductStatus" product_status,
                   "EffectiveFromDate"::DATE effective_from,
                   md5(concat_ws('|', "UnitManufacturingCostUSD"::DECIMAL(12,4), "WholesalePriceUSD"::DECIMAL(12,4),
                       "RecommendedRetailPriceUSD"::DECIMAL(12,4), "SafetyStockDays", "ProductStatus")) scd_hash,
                   row_number() OVER (PARTITION BY "MedicineID" ORDER BY "EffectiveFromDate" DESC, _load_id DESC) rn
            FROM new_master) WHERE rn = 1""")
    # 1) brand-new medicines -> version 1 (valid from the beginning of time)
    new_rows = con.execute("""
        INSERT INTO dim_medicine
        SELECT nextval('seq_medicine_key'), s.* EXCLUDE (effective_from, scd_hash),
               DATE '1900-01-01', DATE '9999-12-31', TRUE, 1, s.scd_hash
        FROM src_med s WHERE NOT EXISTS (SELECT 1 FROM dim_medicine d WHERE d.medicine_id = s.medicine_id)
    """).fetchone()[0]
    # 2) changed medicines -> close current version, open a new one
    con.execute("""
        CREATE OR REPLACE TEMP TABLE changed AS
        SELECT s.*, d.medicine_key old_key, d.version_no old_version
        FROM src_med s JOIN dim_medicine d ON d.medicine_id = s.medicine_id AND d.is_current
        WHERE d.scd_hash <> s.scd_hash""")
    changed = con.execute("SELECT count(*) FROM changed").fetchone()[0]
    if changed:
        con.execute("""UPDATE dim_medicine SET valid_to = c.effective_from - INTERVAL 1 DAY, is_current = FALSE
                       FROM changed c WHERE dim_medicine.medicine_key = c.old_key""")
        con.execute("""
            INSERT INTO dim_medicine
            SELECT nextval('seq_medicine_key'), c.* EXCLUDE (effective_from, scd_hash, old_key, old_version),
                   c.effective_from, DATE '9999-12-31', TRUE, c.old_version + 1, c.scd_hash
            FROM changed c""")
    # 3) Type 1 attributes (names, descriptions) overwrite every version
    con.execute("""UPDATE dim_medicine SET generic_name = s.generic_name, brand_name = s.brand_name
                   FROM src_med s WHERE dim_medicine.medicine_id = s.medicine_id""")
    return {"new_medicines": new_rows, "scd2_new_versions": changed}


def load_dim_pharmacy(con):
    con.execute("""
        CREATE OR REPLACE TEMP TABLE src_ph AS
        SELECT DISTINCT ON ("PharmacyID") "PharmacyID" pharmacy_id, "PharmacyGLN" pharmacy_gln,
               "PharmacyName" pharmacy_name, "PharmacyType" pharmacy_type, "PharmacyCity" city,
               "PharmacyState" state, "CountryCode" country_code, "HospitalID" hospital_id
        FROM new_sales ORDER BY "PharmacyID", "SalesTimestampUTC" DESC""")
    con.execute("""UPDATE dim_pharmacy SET pharmacy_name = s.pharmacy_name, pharmacy_type = s.pharmacy_type,
                   city = s.city, state = s.state FROM src_ph s WHERE dim_pharmacy.pharmacy_id = s.pharmacy_id""")
    con.execute("""
        INSERT INTO dim_pharmacy
        SELECT (SELECT coalesce(max(pharmacy_key), 0) FROM dim_pharmacy) + row_number() OVER (ORDER BY pharmacy_id), *
        FROM src_ph s WHERE NOT EXISTS (SELECT 1 FROM dim_pharmacy d WHERE d.pharmacy_id = s.pharmacy_id)""")


def load_dim_hospital(con):
    con.execute("""
        CREATE OR REPLACE TEMP TABLE src_h AS
        SELECT DISTINCT ON ("HospitalID") "HospitalID" hospital_id, "HospitalGLN" hospital_gln,
               "HospitalName" hospital_name, "HospitalType" hospital_type, "HospitalPharmacyID" hospital_pharmacy_id,
               "City" city, "State" state, "LicensedBeds" licensed_beds, "ICUBeds" icu_beds,
               "AverageOccupancyRatePct" avg_occupancy_pct, coalesce("TraumaLevel", 'None') trauma_level,
               "CatchmentPopulation" catchment_population,
               CASE WHEN "LicensedBeds" >= 600 THEN 'Large (600+ beds)'
                    WHEN "LicensedBeds" >= 300 THEN 'Medium (300-599 beds)' ELSE 'Small (<300 beds)' END bed_size_band
        FROM new_hospital ORDER BY "HospitalID", "DemandMonth" DESC""")
    con.execute("""UPDATE dim_hospital SET licensed_beds = s.licensed_beds, icu_beds = s.icu_beds,
                   avg_occupancy_pct = s.avg_occupancy_pct, bed_size_band = s.bed_size_band
                   FROM src_h s WHERE dim_hospital.hospital_id = s.hospital_id""")
    con.execute("""
        INSERT INTO dim_hospital
        SELECT (SELECT greatest(coalesce(max(hospital_key), 0), 0) FROM dim_hospital) + row_number() OVER (ORDER BY hospital_id), *
        FROM src_h s WHERE NOT EXISTS (SELECT 1 FROM dim_hospital d WHERE d.hospital_id = s.hospital_id)""")


def load_junk_dims(con):
    con.execute("""
        CREATE OR REPLACE TEMP TABLE src_prof AS
        SELECT DISTINCT "SalesChannel" sales_channel,
               CASE WHEN "SalesChannel" = 'Ecommerce' THEN 'E-commerce'
                    WHEN "PharmacyType" = 'Hospital Pharmacy' THEN 'Hospital' ELSE 'Retail' END channel_group,
               "CustomerSegment" customer_segment, "PayerType" payer_type, "PaymentMethod" payment_method,
               "PrescriptionRequiredFlag" prescription_required_flag, coalesce("PromotionCode", 'NONE') promotion_code,
               "EcommerceDeliveryStatus" ecommerce_delivery_status, "ReturnFlag" return_flag, "SaleStatus" sale_status
        FROM new_sales""")
    con.execute("""
        INSERT INTO dim_sales_profile
        SELECT (SELECT coalesce(max(sales_profile_key), 0) FROM dim_sales_profile) + row_number() OVER (), s.*,
               md5(concat_ws('|', s.sales_channel, s.channel_group, s.customer_segment, s.payer_type, s.payment_method, s.prescription_required_flag, s.promotion_code, s.ecommerce_delivery_status, s.return_flag, s.sale_status)) h
        FROM src_prof s WHERE md5(concat_ws('|', s.sales_channel, s.channel_group, s.customer_segment, s.payer_type, s.payment_method, s.prescription_required_flag, s.promotion_code, s.ecommerce_delivery_status, s.return_flag, s.sale_status)) NOT IN (SELECT profile_hash FROM dim_sales_profile)""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE src_ds AS
        SELECT DISTINCT "MedicationRequestStatus" medication_request_status, "FormularyStatus" formulary_status
        FROM new_hospital""")
    con.execute("""
        INSERT INTO dim_demand_status
        SELECT (SELECT coalesce(max(demand_status_key), 0) FROM dim_demand_status) + row_number() OVER (), s.*,
               md5(concat_ws('|', s.medication_request_status, s.formulary_status))
        FROM src_ds s WHERE md5(concat_ws('|', s.medication_request_status, s.formulary_status)) NOT IN (SELECT status_hash FROM dim_demand_status)""")


def _junk_partial(con, stats):
    """Only one of the two sources has new rows: create an empty stand-in for the other."""
    missing = "hospital" if stats["new_sales_rows"] else "sales"
    if missing == "hospital":
        con.execute('CREATE OR REPLACE TEMP TABLE new_hospital AS SELECT NULL::VARCHAR "MedicationRequestStatus", NULL::VARCHAR "FormularyStatus" WHERE FALSE')
    else:
        con.execute('CREATE OR REPLACE TEMP TABLE new_sales AS SELECT ' + ", ".join(
            f'NULL::VARCHAR "{c}"' for c in ["SalesChannel", "PharmacyType", "CustomerSegment", "PayerType",
            "PaymentMethod", "PrescriptionRequiredFlag", "PromotionCode", "EcommerceDeliveryStatus",
            "ReturnFlag", "SaleStatus"]) + " WHERE FALSE")
    load_junk_dims(con)


# ----------------------------------------------------------------------------- facts
def load_fact_sales(con) -> int:
    before = con.execute("SELECT count(*) FROM fact_sales").fetchone()[0]
    con.execute("""
        INSERT OR IGNORE INTO fact_sales
        SELECT s."SalesTransactionID",
               CAST(strftime(s."SalesDate"::DATE, '%Y%m%d') AS INTEGER),
               CAST(strftime(s."PharmacyDeliveryDate"::DATE, '%Y%m%d') AS INTEGER),
               CAST(strftime(s."BatchExpiryDate"::DATE, '%Y%m%d') AS INTEGER),
               m.medicine_key,
               coalesce(ph.pharmacy_key, -1),
               coalesce(h.hospital_key, -1),
               pr.sales_profile_key,
               s."PrescriptionID", s."PharmacyDeliveryID", s."WarehouseLotID", s."ProductionBatchID",
               s."SourceWarehouseID", s."SalesTimestampUTC"::TIMESTAMP,
               s."QuantitySoldPacks", s."UnitRetailPriceUSD", s."GrossSalesAmountUSD", s."DiscountAmountUSD",
               s."NetSalesAmountUSD", s."SalesTaxUSD", s."CostOfGoodsSoldUSD", s."GrossMarginUSD",
               s."ReturnedQuantityPacks", s."DaysToExpiryAtSale", s._load_id
        FROM new_sales s
        JOIN dim_medicine m ON m.medicine_id = s."MedicineID"
                           AND s."SalesDate"::DATE BETWEEN m.valid_from AND m.valid_to   -- SCD2 lookup
        LEFT JOIN dim_pharmacy ph ON ph.pharmacy_id = s."PharmacyID"
        LEFT JOIN dim_hospital h  ON h.hospital_id = s."HospitalID"
        JOIN dim_sales_profile pr ON pr.profile_hash = md5(concat_ws('|', s."SalesChannel",
               CASE WHEN s."SalesChannel" = 'Ecommerce' THEN 'E-commerce'
                    WHEN s."PharmacyType" = 'Hospital Pharmacy' THEN 'Hospital' ELSE 'Retail' END,
               s."CustomerSegment", s."PayerType", s."PaymentMethod", s."PrescriptionRequiredFlag",
               coalesce(s."PromotionCode", 'NONE'), s."EcommerceDeliveryStatus", s."ReturnFlag", s."SaleStatus"))
        ORDER BY 2   -- keep the fact physically sorted by date -> DuckDB zone maps skip old row groups
    """)
    return con.execute("SELECT count(*) FROM fact_sales").fetchone()[0] - before


def load_fact_demand(con) -> int:
    before = con.execute("SELECT count(*) FROM fact_hospital_demand").fetchone()[0]
    con.execute("""
        INSERT OR IGNORE INTO fact_hospital_demand
        SELECT s."HospitalDemandRecordID",
               CAST(strftime(s."DemandMonth"::DATE, '%Y%m%d') AS INTEGER),
               coalesce(h.hospital_key, -1), m.medicine_key, ds.demand_status_key,
               s."EstimatedDiseaseCases", s."DiseaseRelatedAdmissions", s."OutpatientEncounters",
               s."ForecastDemandPacks", s."RequestedQuantityPacks", s."ApprovedQuantityPacks",
               s."ReceivedQuantityPacks", s."EmergencyRequestQuantityPacks", s."OpeningStockPacks",
               s."DispensedQuantityPacks", s."ConsumedQuantityPacks", s."WastageQuantityPacks",
               s."ClosingStockPacks", s."UnfulfilledDemandPacks", s."StockoutDays", s."ServiceLevelPct",
               s."AverageLeadTimeDays", s."DemandTrendIndex", s._load_id
        FROM new_hospital s
        JOIN dim_medicine m ON m.medicine_id = s."MedicineID"
                           AND s."DemandMonth"::DATE BETWEEN m.valid_from AND m.valid_to
        LEFT JOIN dim_hospital h ON h.hospital_id = s."HospitalID"
        JOIN dim_demand_status ds ON ds.status_hash = md5(concat_ws('|', s."MedicationRequestStatus", s."FormularyStatus"))
        ORDER BY 2""")
    return con.execute("SELECT count(*) FROM fact_hospital_demand").fetchone()[0] - before


def build_aggregates_and_partitions(con):
    con.execute((SQL_DIR / "20_aggregates.sql").read_text())
    for col in ("sales_date_key", "medicine_key"):
        con.execute(f"CREATE INDEX IF NOT EXISTS ix_fact_sales_{col} ON fact_sales ({col})")
    con.execute("CREATE INDEX IF NOT EXISTS ix_fact_dem_month ON fact_hospital_demand (month_date_key)")
    pq = WAREHOUSE_DIR / "parquet" / "fact_sales"
    pq.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""COPY (SELECT f.*, d.year_num, d.month_num FROM fact_sales f JOIN dim_date d ON d.date_key = f.sales_date_key)
                    TO '{pq}' (FORMAT PARQUET, PARTITION_BY (year_num, month_num), OVERWRITE_OR_IGNORE)""")


# ----------------------------------------------------------------------------- driver
def run_transform(refresh_id: int | None = None) -> dict:
    started = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    wm = _watermarks()
    stats = {}
    con = duck_conn()
    try:
        init_mart(con)
        new_max = {}
        with tempfile.TemporaryDirectory() as td:
            for k in PHASE1:
                n, new_max[k] = _extract(con, k, wm.get(k, 0), Path(td))
                stats[f"new_{k}_rows"] = n
            con.execute("BEGIN TRANSACTION")
            if stats["new_master_rows"]:
                stats.update(load_dim_medicine(con))
            if stats["new_hospital_rows"]:
                load_dim_hospital(con)
            if stats["new_sales_rows"] or stats["new_hospital_rows"]:
                load_junk_dims(con) if stats["new_sales_rows"] and stats["new_hospital_rows"] else _junk_partial(con, stats)
            if stats["new_sales_rows"]:
                load_dim_pharmacy(con)
                stats["fact_sales_inserted"] = load_fact_sales(con)
            if stats["new_hospital_rows"]:
                stats["fact_demand_inserted"] = load_fact_demand(con)
            con.execute("COMMIT")
        build_aggregates_and_partitions(con)
        stats["fact_sales_total"] = con.execute("SELECT count(*) FROM fact_sales").fetchone()[0]
        stats["fact_demand_total"] = con.execute("SELECT count(*) FROM fact_hospital_demand").fetchone()[0]
        stats["dim_medicine_rows"] = con.execute("SELECT count(*) FROM dim_medicine").fetchone()[0]
    finally:
        con.close()
    with pg_conn() as conn, conn.cursor() as cur:
        for k, v in new_max.items():
            cur.execute("UPDATE ops.etl_watermark SET last_load_id=%s, updated_at=now() WHERE source_key=%s", (v, k))
        import json
        stats["duration_s"] = round(time.perf_counter() - t0, 2)
        cur.execute("INSERT INTO ops.pipeline_run (refresh_id, step, started_at, ended_at, status, details)"
                    " VALUES (%s,'transform_sdi_mart',%s,now(),'SUCCESS',%s)", (refresh_id, started, json.dumps(stats)))
    return stats


if __name__ == "__main__":
    print(run_transform())
