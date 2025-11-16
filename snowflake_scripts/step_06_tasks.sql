-- Task graph to orchestrate dbt runs in Snowflake Workspace
-- Replace PROJECT identifier with your Workspace dbt project name if different.

USE ROLE DBT_ETL;
USE DATABASE ANALYTICS;
CREATE SCHEMA IF NOT EXISTS ANALYTICS.SCHEDULE;
USE SCHEMA SCHEDULE;

---------------------------------------------------
-- 1) DEPS  — root (scheduled)
---------------------------------------------------
CREATE OR REPLACE TASK DBT_PIPELINE_DEPS
  WAREHOUSE = DBT_WH_M
  SCHEDULE = 'USING CRON 1 10 * * * America/New_York'
AS
  EXECUTE DBT PROJECT SNOWFLAKE_DBT_STACK 
    ARGS='deps' 
    PROJECT_ROOT='/dbt_pipeline';


---------------------------------------------------
-- 2) SEED  — depends on DEPS
---------------------------------------------------
CREATE OR REPLACE TASK DBT_PIPELINE_SEED
  WAREHOUSE = DBT_WH_M
  AFTER DBT_PIPELINE_DEPS
AS
  EXECUTE DBT PROJECT SNOWFLAKE_DBT_STACK 
    ARGS='seed --select nyc_taxi --full-refresh --target dev' 
    PROJECT_ROOT='/dbt_pipeline';


---------------------------------------------------
-- 3) INGEST  — depends on SEED
-- Runs custom macro operation to ingest raw files
---------------------------------------------------
CREATE OR REPLACE TASK DBT_PIPELINE_INGEST
  WAREHOUSE = DBT_WH_M
  AFTER DBT_PIPELINE_SEED
AS
  EXECUTE DBT PROJECT SNOWFLAKE_DBT_STACK
    ARGS='run-operation ingest_nyc_tlc_from_stage --target dev' 
    PROJECT_ROOT='/dbt_pipeline';


---------------------------------------------------
-- 4) RUN LAYERS  — depends on INGEST
-- Builds staging → intermediate → core → marts
---------------------------------------------------
CREATE OR REPLACE TASK DBT_PIPELINE_RUN
  WAREHOUSE = DBT_WH_M
  AFTER DBT_PIPELINE_INGEST
AS
  EXECUTE DBT PROJECT SNOWFLAKE_DBT_STACK
    ARGS='run --target dev --select 
          staging.nyc_taxi.stg_tran_nyc_taxi_* 
          intermediate.nyc_taxi.int_tran_nyc_taxi_all 
          core.nyc_taxi.dim_* 
          core.nyc_taxi.fact_nyc_taxi_trips 
          mart_trips_daily 
          mart_zone_flow' 
    PROJECT_ROOT='/dbt_pipeline';


---------------------------------------------------
-- 5) TEST  — depends on RUN
---------------------------------------------------
CREATE OR REPLACE TASK DBT_PIPELINE_TEST
  WAREHOUSE = DBT_WH_M
  AFTER DBT_PIPELINE_RUN
AS
  EXECUTE DBT PROJECT SNOWFLAKE_DBT_STACK
    ARGS='test --target dev' 
    PROJECT_ROOT='/dbt_pipeline';


---------------------------------------------------
-- 6) SNAPSHOT  — final step
---------------------------------------------------
CREATE OR REPLACE TASK DBT_PIPELINE_SNAPSHOT
  WAREHOUSE = DBT_WH_M
  AFTER DBT_PIPELINE_TEST
AS
  EXECUTE DBT PROJECT SNOWFLAKE_DBT_STACK
    ARGS='snapshot --target dev' 
    PROJECT_ROOT='/dbt_pipeline';


-- Enable and trigger
ALTER TASK DBT_PIPELINE_DEPS RESUME;
ALTER TASK DBT_PIPELINE_SEED RESUME;
ALTER TASK DBT_PIPELINE_INGEST RESUME;
ALTER TASK DBT_PIPELINE_RUN RESUME;
ALTER TASK DBT_PIPELINE_TEST RESUME;
ALTER TASK DBT_PIPELINE_SNAPSHOT RESUME;

-- On-demand run from DEPS root (optional)
-- EXECUTE TASK DBT_PIPELINE_DEPS;

