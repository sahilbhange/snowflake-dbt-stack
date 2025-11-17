# snowflake-dbt-stack

## Quickstart

1. Clone repo and enter workspace.
2. Create and activate the dbt virtualenv: `python -m venv .venv_dbt && source .venv_dbt/bin/activate`.
3. Install dbt Snowflake locally: `pip install dbt-snowflake`.
4. Set the profile directory and required Snowflake env vars. The profile now uses environment variables for account, user, role, warehouse, database, schema, and private key, so export them once per shell (consider wrapping them in a script like `snowflake_scripts/env.sh`). Make sure the private key path points to an existing file; `~` is not expanded inside dbt, so use the absolute path or `realpath` output:
   ```bash
   export DBT_PROFILES_DIR="$(pwd)/dbt_pipeline"
   export SNOWFLAKE_ACCOUNT=JNAEVTS-ZZC51360
   export SNOWFLAKE_USER=DBT_SVC
   export SNOWFLAKE_ROLE=DBT_ETL
   export SNOWFLAKE_WAREHOUSE=DBT_WH_M
   export SNOWFLAKE_DATABASE=ANALYTICS
   export SNOWFLAKE_SCHEMA=RAW
   export SNOWFLAKE_PRIVATE_KEY_PATH=/Users/sahilbhange/.ssh/snowflake_keys/dbt_svc_rsa_key.p8
    ls -l "$SNOWFLAKE_PRIVATE_KEY_PATH"  # verify the file exists before running dbt
   # Optionally expose the passphrase if your private key is encrypted:
   export SNOWSQL_PRIVATE_KEY_PASSPHRASE=<passphrase>
   ```
5. Run the dbt workflow inside the project dir (note the path contains a space, so wrap it in quotes):
   ```bash

   source .venv_dbt/bin/activate
   cd dbt_pipeline
   export DBT_PROFILES_DIR="$(pwd)"
   cd dbt_pipeline
   dbt deps
   dbt seed --select nyc_taxi
   dbt run-operation ingest_nyc_tlc_from_stage
   dbt run --select 'staging.nyc_taxi.stg_tran_nyc_taxi_*'
  

   # build the intermediate union
   dbt run --select intermediate.nyc_taxi.int_tran_nyc_taxi_all

   # build the dimension models
   dbt run --select 'core.nyc_taxi.dim_*'

   # build the fact table models
   dbt run --select core.nyc_taxi.fact_nyc_taxi_trips

   dbt run --select mart_trips_daily mart_zone_flow

   dbt test
   dbt snapshot
   dbt docs generate
   ```

   Run multiple DBT steps at Once
   dbt run --target dev && snapshot --target dev && seed --target dev


   Alternatively you can stay in the repo root and prefix each command with `dbt --project-dir dbt_pipeline`; just keep the flag on the same line (no stray newlines) so dbt parses the argument correctly.

## Project Layout
- `snowflake_scripts/` – raw SQL helpers (initial roles, manual ingest, workflow notes)
- `dbt_pipeline/` – dbt project: macros, models, snapshots, seeds, packages
- `plan.md` – living plan for the end-to-end state-of-the-art build


