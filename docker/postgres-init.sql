-- runs once when the Postgres container is first created
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
-- read-only user for Grafana dashboards
CREATE USER grafana_reader WITH PASSWORD 'grafana';
GRANT CONNECT ON DATABASE pharma_dwh TO grafana_reader;
\connect pharma_dwh
CREATE SCHEMA IF NOT EXISTS ops AUTHORIZATION pharma;
CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION pharma;
GRANT USAGE ON SCHEMA ops, staging TO grafana_reader;
ALTER DEFAULT PRIVILEGES FOR USER pharma IN SCHEMA ops GRANT SELECT ON TABLES TO grafana_reader;
ALTER DEFAULT PRIVILEGES FOR USER pharma IN SCHEMA staging GRANT SELECT ON TABLES TO grafana_reader;
