-- =====================================================================
-- Phase 1 data mart: Sales & Demand Intelligence (SDI)  -- DuckDB (OLAP)
-- Two fact tables share conformed dimensions (dim_date, dim_medicine,
-- dim_hospital), so this mart can later join the Phase 2/3 marts in the
-- fact constellation.
-- =====================================================================

-- ---------- Date dimension (role-playing: sale date, pharmacy delivery date,
-- ---------- batch expiry date, demand month all point here) -------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_key      INTEGER PRIMARY KEY,      -- yyyymmdd
    full_date     DATE NOT NULL,
    day_of_month  SMALLINT,
    day_of_week   SMALLINT,                 -- 1 = Monday
    day_name      VARCHAR,
    is_weekend    BOOLEAN,
    week_of_year  SMALLINT,
    month_num     SMALLINT,
    month_name    VARCHAR,
    quarter_num   SMALLINT,
    year_num      SMALLINT,
    year_month    VARCHAR                   -- '2026-01'
);

-- ---------- Medicine: Slowly Changing Dimension Type 2 -------------------
-- Prices, cost, safety stock and status keep history (new row per change);
-- names/descriptions are Type 1 (overwritten on every version).
CREATE SEQUENCE IF NOT EXISTS seq_medicine_key START 1;
CREATE TABLE IF NOT EXISTS dim_medicine (
    medicine_key           INTEGER PRIMARY KEY,
    medicine_id            VARCHAR NOT NULL,     -- natural / business key
    gtin14                 VARCHAR,
    generic_name           VARCHAR,
    brand_name             VARCHAR,
    disease_category       VARCHAR,
    icd10_category         VARCHAR,
    atc_code               VARCHAR,
    therapeutic_class      VARCHAR,
    dosage_form            VARCHAR,
    strength               VARCHAR,
    route                  VARCHAR,
    pack_size              INTEGER,
    prescription_class     VARCHAR,
    controlled_flag        VARCHAR,
    cold_chain_flag        VARCHAR,
    storage_min_temp_c     INTEGER,
    storage_max_temp_c     INTEGER,
    shelf_life_months      INTEGER,
    -- Type 2 tracked attributes
    unit_mfg_cost_usd      DECIMAL(12,4),
    wholesale_price_usd    DECIMAL(12,4),
    retail_price_usd       DECIMAL(12,4),
    safety_stock_days      INTEGER,
    product_status         VARCHAR,
    -- SCD2 housekeeping
    valid_from             DATE NOT NULL,
    valid_to               DATE NOT NULL,        -- 9999-12-31 for the current row
    is_current             BOOLEAN NOT NULL,
    version_no             INTEGER NOT NULL,
    scd_hash               VARCHAR NOT NULL
);

-- ---------- Pharmacy (Type 1) --------------------------------------------
CREATE TABLE IF NOT EXISTS dim_pharmacy (
    pharmacy_key   INTEGER PRIMARY KEY,
    pharmacy_id    VARCHAR NOT NULL,
    pharmacy_gln   VARCHAR,
    pharmacy_name  VARCHAR,
    pharmacy_type  VARCHAR,
    city           VARCHAR,
    state          VARCHAR,
    country_code   VARCHAR,
    hospital_id    VARCHAR                  -- filled only for hospital pharmacies
);

-- ---------- Hospital (Type 1, conformed with Phase 2/3) -------------------
CREATE TABLE IF NOT EXISTS dim_hospital (
    hospital_key           INTEGER PRIMARY KEY,
    hospital_id            VARCHAR NOT NULL,
    hospital_gln           VARCHAR,
    hospital_name          VARCHAR,
    hospital_type          VARCHAR,
    hospital_pharmacy_id   VARCHAR,
    city                   VARCHAR,
    state                  VARCHAR,
    licensed_beds          INTEGER,
    icu_beds               INTEGER,
    avg_occupancy_pct      DECIMAL(6,2),
    trauma_level           VARCHAR,
    catchment_population   BIGINT,
    bed_size_band          VARCHAR           -- derived: Small / Medium / Large
);

-- ---------- Junk dimension: low-cardinality flags of a sale ---------------
-- Instead of 9 tiny dimensions, every combination that actually occurs gets one key.
CREATE TABLE IF NOT EXISTS dim_sales_profile (
    sales_profile_key           INTEGER PRIMARY KEY,
    sales_channel               VARCHAR,      -- Physical Pharmacy / Ecommerce (source)
    channel_group               VARCHAR,      -- Retail / Hospital / E-commerce (derived)
    customer_segment            VARCHAR,
    payer_type                  VARCHAR,
    payment_method              VARCHAR,
    prescription_required_flag  VARCHAR,
    promotion_code              VARCHAR,
    ecommerce_delivery_status   VARCHAR,
    return_flag                 VARCHAR,
    sale_status                 VARCHAR,
    profile_hash                VARCHAR UNIQUE
);

-- ---------- Junk dimension for hospital demand rows ------------------------
CREATE TABLE IF NOT EXISTS dim_demand_status (
    demand_status_key          INTEGER PRIMARY KEY,
    medication_request_status  VARCHAR,
    formulary_status           VARCHAR,
    status_hash                VARCHAR UNIQUE
);

-- ---------- FACT 1: transaction fact, grain = one sales transaction --------
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_transaction_id   VARCHAR PRIMARY KEY,   -- degenerate dimension
    sales_date_key         INTEGER NOT NULL,      -- role: sale date
    delivery_date_key      INTEGER,               -- role: pharmacy replenishment date
    expiry_date_key        INTEGER,               -- role: batch expiry date
    medicine_key           INTEGER NOT NULL,      -- SCD2 version valid on the sale date
    pharmacy_key           INTEGER NOT NULL,
    hospital_key           INTEGER NOT NULL,      -- -1 = not a hospital pharmacy
    sales_profile_key      INTEGER NOT NULL,
    prescription_id        VARCHAR,               -- degenerate
    pharmacy_delivery_id   VARCHAR,               -- degenerate
    warehouse_lot_id       VARCHAR,               -- degenerate (lineage to Phase 2/3)
    production_batch_id    VARCHAR,               -- degenerate (lineage to Phase 2/3)
    source_warehouse_id    VARCHAR,
    sales_ts_utc           TIMESTAMP,
    quantity_packs         INTEGER,
    unit_price_usd         DECIMAL(12,4),
    gross_sales_usd        DECIMAL(14,4),
    discount_usd           DECIMAL(14,4),
    net_sales_usd          DECIMAL(14,4),
    sales_tax_usd          DECIMAL(14,4),
    cogs_usd               DECIMAL(14,4),
    gross_margin_usd       DECIMAL(14,4),
    returned_packs         INTEGER,
    days_to_expiry_at_sale INTEGER,
    load_id                BIGINT
);

-- ---------- FACT 2: periodic snapshot, grain = hospital x medicine x month --
CREATE TABLE IF NOT EXISTS fact_hospital_demand (
    hospital_demand_record_id VARCHAR PRIMARY KEY,
    month_date_key            INTEGER NOT NULL,
    hospital_key              INTEGER NOT NULL,
    medicine_key              INTEGER NOT NULL,
    demand_status_key         INTEGER NOT NULL,
    estimated_disease_cases   INTEGER,
    disease_admissions        INTEGER,
    outpatient_encounters     INTEGER,
    forecast_demand_packs     INTEGER,
    requested_packs           INTEGER,
    approved_packs            INTEGER,
    received_packs            INTEGER,
    emergency_request_packs   INTEGER,
    opening_stock_packs       INTEGER,     -- semi-additive (do not sum across months)
    dispensed_packs           INTEGER,
    consumed_packs            INTEGER,
    wastage_packs             INTEGER,
    closing_stock_packs       INTEGER,     -- semi-additive
    unfulfilled_demand_packs  INTEGER,
    stockout_days             INTEGER,
    service_level_pct         DECIMAL(6,2),   -- non-additive (average it)
    avg_lead_time_days        INTEGER,
    demand_trend_index        DECIMAL(8,4),
    load_id                   BIGINT
);
