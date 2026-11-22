-- =====================================================================
-- Query-optimization layer (rebuilt after every load).
-- DuckDB has no materialized views, so these pre-aggregated summary
-- tables play that role: dashboards read a few thousand rows instead of
-- scanning/joining 500k+ fact rows.
-- =====================================================================

CREATE OR REPLACE TABLE agg_sales_monthly AS
SELECT d.year_month,
       d.year_num,
       d.month_num,
       m.medicine_id,
       m.generic_name,
       m.therapeutic_class,
       m.disease_category,
       m.cold_chain_flag,
       p.channel_group,
       count(*)                     AS txn_count,
       sum(f.quantity_packs)        AS qty_packs,
       sum(f.gross_sales_usd)       AS gross_sales_usd,
       sum(f.net_sales_usd)         AS net_sales_usd,
       sum(f.cogs_usd)              AS cogs_usd,
       sum(f.gross_margin_usd)      AS gross_margin_usd,
       sum(f.returned_packs)        AS returned_packs
FROM fact_sales f
JOIN dim_date d           ON d.date_key = f.sales_date_key
JOIN dim_medicine m       ON m.medicine_key = f.medicine_key
JOIN dim_sales_profile p  ON p.sales_profile_key = f.sales_profile_key
GROUP BY ALL
ORDER BY d.year_month;

CREATE OR REPLACE TABLE agg_demand_monthly AS
SELECT d.year_month,
       h.hospital_id,
       h.hospital_name,
       m.therapeutic_class,
       m.disease_category,
       sum(f.forecast_demand_packs)    AS forecast_packs,
       sum(f.consumed_packs)           AS consumed_packs,
       sum(f.dispensed_packs)          AS dispensed_packs,
       sum(f.unfulfilled_demand_packs) AS unfulfilled_packs,
       sum(f.stockout_days)            AS stockout_days,
       avg(f.service_level_pct)        AS avg_service_level_pct
FROM fact_hospital_demand f
JOIN dim_date d      ON d.date_key = f.month_date_key
JOIN dim_hospital h  ON h.hospital_key = f.hospital_key
JOIN dim_medicine m  ON m.medicine_key = f.medicine_key
GROUP BY ALL
ORDER BY d.year_month;

-- ART indexes on the summary tables' most-filtered columns
CREATE INDEX IF NOT EXISTS ix_agg_sales_ym  ON agg_sales_monthly (year_month);
CREATE INDEX IF NOT EXISTS ix_agg_dem_ym    ON agg_demand_monthly (year_month);
