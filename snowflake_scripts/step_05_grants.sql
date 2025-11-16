
use role securityadmin;

-- Supplemental / environment-specific grants
-- Adjust object names below to match your environment (warehouses/schemas)

GRANT CREATE PROJECT ON SCHEMA ANALYTICS.SCHEDULE TO ROLE DBT_ETL;

-- Example: if you use COMPUTE_WH alongside DBT_WH_*, grant usage/operate
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE DBT_ETL;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE DBT_ETL;

