# Snowflake Setup – How to Reproduce

This folder contains one‑time and recurring SQL scripts to bootstrap your Snowflake environment for the dbt project and to ingest NYC TLC parquet files. The sections below explain purpose, required role, and a recommended execution order so anyone can replicate the setup quickly.

## Files and Execution Order

1) step_01_initial_setup.sql
- Purpose: Core bootstrap – role `DBT_ETL`, warehouses `DBT_WH_M`/`DBT_WH_L`, database `ANALYTICS` with standard schemas, service user `DBT_SVC`, and broad grants (current + future objects).
- Role to use: mix of `SECURITYADMIN`, `ACCOUNTADMIN` (for account‑level task grant), and `SYSADMIN` (for object creation). The script switches roles inline.
- When: Run once per account.

2) step_02_network_policy.sql
- Purpose: Create a NETWORK RULE and EXTERNAL ACCESS INTEGRATION to allow `dbt deps` (Snowflake Workspace) to fetch packages from `hub.getdbt.com` and GitHub.
- Role to use: `ACCOUNTADMIN` (or a role with privileges to create network rules and external access integrations).
- When: Run once if you execute dbt from the Snowflake Workspace.

3) step_03_raw_stage_upload.sql
- Purpose: Create the `ANALYTICS.UTIL` file format + internal stage and show SnowSQL `PUT` examples to upload yellow/green/fhvhv parquet files. Includes `LIST` checks.
- Role to use: `DBT_ETL` or `SYSADMIN` with USAGE on `ANALYTICS.UTIL`.
- When: Any time you add new parquet files and want to stage them for ingestion.

4) step_04_raw_data_load.sql
- Purpose: Create RAW landing tables via `USING TEMPLATE (INFER_SCHEMA(...))` and `COPY INTO` from staged parquet files for each service.
- Role to use: `DBT_ETL` (uses `ANALYTICS.RAW` and `ANALYTICS.UTIL`).
- When: First full backfill, or rerun after new files are staged (or use the dbt macro `run-operation ingest_nyc_tlc_from_stage`).

5) step_05_grants.sql (optional)
- Purpose: Supplemental grants (e.g., on `COMPUTE_WH`, or custom schemas). Verify object names match your environment.
- Role to use: `SECURITYADMIN`.

6) step_06_tasks.sql
- Purpose: Define a Snowflake Task graph that orchestrates dbt end‑to‑end (deps → seed → ingest → run → test → snapshot) using `EXECUTE DBT PROJECT`. The root `DBT_PIPELINE_DEPS` is scheduled; other tasks chain via `AFTER`.
- Role to use: `DBT_ETL` (requires `EXECUTE TASK ON ACCOUNT` granted in step_01). Ensure the Workspace dbt project name (e.g., `SNOWFLAKE_DBT_STACK`) and `PROJECT_ROOT='/dbt_pipeline'` match your environment.

## Notes & Gotchas
- Path quoting: When using SnowSQL `PUT`, wrap file URIs with spaces in single quotes and escape the space in shell (or avoid spaces).
- Parquet timestamps: If epochs land as microseconds, convert with `DATEADD('microsecond', epoch, TO_TIMESTAMP_NTZ('1970-01-01 00:00:00'))` in staging models.
- Case sensitivity: `INFER_SCHEMA` may create uppercase identifiers (e.g., `VENDORID`). Quote names in staging or normalize column names.
- Transient vs. permanent: dbt may compile to `TRANSIENT` tables by default. Set `transient: false` in model configs if you require permanent tables.

## Validation Snippets
- Verify stage uploads:
  ```sql
  LIST @ANALYTICS.UTIL.NYC_TLC_STAGE/yellow;
  ```
- Verify RAW counts:
  ```sql
  SELECT count(*) FROM ANALYTICS.RAW.raw_tran_nyc_taxi_yellow;
  ```
- Check grants (quick):
  ```sql
  SHOW GRANTS TO ROLE DBT_ETL;
  ```

---
Use these scripts to replicate the Snowflake setup for the dbt project. See the top‑level README and docs/GETTING_STARTED.md for end‑to‑end dbt run instructions.
