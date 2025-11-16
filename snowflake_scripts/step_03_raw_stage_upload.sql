-- NYC TLC Parquet ingest workflow (documented runbook)
-- This file captures the exact steps we followed to land yellow/green/fhvhv trip data
-- into Snowflake using local downloads + SnowSQL PUT + COPY INTO.

-- ============================================================================
-- 1. Download files locally (run in terminal, not Snowflake)
-- ============================================================================
-- mkdir -p "${PWD}/data/nyc_tlc/yellow" "${PWD}/data/nyc_tlc/green" "${PWD}/data/nyc_tlc/fhvhv"
-- curl -L -o "data/nyc_tlc/yellow/yellow_tripdata_2022-01.parquet" \
--   "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"
-- curl -L -o "data/nyc_tlc/green/green_tripdata_2022-01.parquet" \
--   "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet"
-- curl -L -o "data/nyc_tlc/fhvhv/fhvhv_tripdata_2022-01.parquet" \
--   "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-01.parquet"

-- ============================================================================
-- 2. SnowSQL session (run `snowsql -c sahil` or `snowsql -c dbt_svc`)
-- ============================================================================
USE ROLE DBT_ETL;
USE WAREHOUSE DBT_WH_M;
USE DATABASE ANALYTICS;
USE SCHEMA UTIL;

-- Create file format + internal stage
CREATE OR REPLACE FILE FORMAT NYC_TLC_PARQUET_FF
  TYPE = PARQUET;

CREATE OR REPLACE STAGE NYC_TLC_STAGE
  FILE_FORMAT = NYC_TLC_PARQUET_FF
  COMMENT = 'Internal stage for locally uploaded NYC TLC tripdata parquet files';

-- Upload local Parquet files (absolute paths; quote strings to handle spaces)
PUT 'file:///Users/sahilbhange/Desktop/DE Work/snowflake-dbt-stack/data/nyc_tlc/yellow/yellow_tripdata_2022-01.parquet'
    @ANALYTICS.UTIL.NYC_TLC_STAGE/yellow
    AUTO_COMPRESS = FALSE;

PUT 'file:///Users/sahilbhange/Desktop/DE Work/snowflake-dbt-stack/data/nyc_tlc/green/green_tripdata_2022-01.parquet'
    @ANALYTICS.UTIL.NYC_TLC_STAGE/green
    AUTO_COMPRESS = FALSE;

PUT 'file:///Users/sahilbhange/Desktop/DE Work/snowflake-dbt-stack/data/nyc_tlc/fhvhv/fhvhv_tripdata_2022-01.parquet'
    @ANALYTICS.UTIL.NYC_TLC_STAGE/fhvhv
    AUTO_COMPRESS = FALSE;

-- Confirm staged files
LIST @ANALYTICS.UTIL.NYC_TLC_STAGE/yellow;
LIST @ANALYTICS.UTIL.NYC_TLC_STAGE/green;
LIST @ANALYTICS.UTIL.NYC_TLC_STAGE/fhvhv;

-- ============================================================================
-- 3. Load staged files into RAW tables via dbt (preferred ongoing path)
-- ============================================================================
-- Run from project root:
--   export DBT_PROFILES_DIR=./dbt_pipeline
--   dbt debug
--   dbt run-operation ingest_nyc_tlc_from_stage
-- The macro will:
--   * Ensure file format + stage exist (idempotent)
--   * Create RAW tables via INFER_SCHEMA template
--   * COPY INTO raw_tran_nyc_taxi_yellow/green/fhvhv tables from the internal stage

-- 4. (Optional) Direct SQL load (legacy path retained)
-- ============================================================================
-- See snowflake_scripts/step_04_raw_data_load.sql if you need to perform the CTAS + COPY steps outside dbt.

