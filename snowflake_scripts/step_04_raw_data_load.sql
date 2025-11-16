-- NYC TLC local ingest workflow (run after uploading Parquet files via PUT)

-- snowsql -c sahil

USE ROLE DBT_ETL;

USE WAREHOUSE DBT_WH_M;
USE DATABASE ANALYTICS;

-- Utilities: file format + internal stage for NYC TLC data
USE SCHEMA UTIL;

-- Safer object creation: do not replace after uploading, as REPLACE wipes files
CREATE SCHEMA IF NOT EXISTS ANALYTICS.UTIL;
CREATE FILE FORMAT IF NOT EXISTS NYC_TLC_PARQUET_FF
  TYPE = PARQUET;

CREATE STAGE IF NOT EXISTS NYC_TLC_STAGE
  FILE_FORMAT = NYC_TLC_PARQUET_FF
  COMMENT = 'Internal stage for locally uploaded NYC TLC tripdata parquet files';

-- After running SnowSQL PUT commands such as:
--   PUT file://<local_path>/nyc_tlc/yellow/*.parquet @ANALYTICS.UTIL.NYC_TLC_STAGE/yellow AUTO_COMPRESS = FALSE;
--   PUT file://<local_path>/nyc_tlc/green/*.parquet  @ANALYTICS.UTIL.NYC_TLC_STAGE/green  AUTO_COMPRESS = FALSE;
--   PUT file://<local_path>/nyc_tlc/fhvhv/*.parquet  @ANALYTICS.UTIL.NYC_TLC_STAGE/fhvhv  AUTO_COMPRESS = FALSE;
-- execute the CTAS + COPY steps below to land data into RAW tables.

-- Create RAW schema tables inferred from Parquet schema metadata
USE SCHEMA RAW;

CREATE OR REPLACE TABLE raw_tran_nyc_taxi_yellow
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION => '@ANALYTICS.UTIL.NYC_TLC_STAGE/yellow',
      FILE_FORMAT => 'ANALYTICS.UTIL.NYC_TLC_PARQUET_FF'
    )
  )
);

CREATE OR REPLACE TABLE raw_tran_nyc_taxi_green
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION => '@ANALYTICS.UTIL.NYC_TLC_STAGE/green',
      FILE_FORMAT => 'ANALYTICS.UTIL.NYC_TLC_PARQUET_FF'
    )
  )
);

CREATE OR REPLACE TABLE raw_tran_nyc_taxi_fhvhv
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION => '@ANALYTICS.UTIL.NYC_TLC_STAGE/fhvhv',
      FILE_FORMAT => 'ANALYTICS.UTIL.NYC_TLC_PARQUET_FF'
    )
  )
);

-- Load staged parquet files into RAW tables
COPY INTO raw_tran_nyc_taxi_yellow
  FROM @ANALYTICS.UTIL.NYC_TLC_STAGE/yellow
  FILE_FORMAT = (FORMAT_NAME = 'ANALYTICS.UTIL.NYC_TLC_PARQUET_FF')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  ON_ERROR = CONTINUE;

COPY INTO raw_tran_nyc_taxi_green
  FROM @ANALYTICS.UTIL.NYC_TLC_STAGE/green
  FILE_FORMAT = (FORMAT_NAME = 'ANALYTICS.UTIL.NYC_TLC_PARQUET_FF')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  ON_ERROR = CONTINUE;

COPY INTO raw_tran_nyc_taxi_fhvhv
  FROM @ANALYTICS.UTIL.NYC_TLC_STAGE/fhvhv
  FILE_FORMAT = (FORMAT_NAME = 'ANALYTICS.UTIL.NYC_TLC_PARQUET_FF')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  ON_ERROR = CONTINUE;

