# snowflake-dbt-stack

## Quickstart

1. Clone repo and enter workspace.
2. Create and activate the dbt virtualenv: `python -m venv .venv_dbt && source .venv_dbt/bin/activate`.
3. Install dbt Snowflake locally: `pip install dbt-snowflake`.
4. Set the profile directory and required Snowflake env vars. The profile now uses environment variables for account, user, role, warehouse, database, schema, and private key, so export them once per shell (consider wrapping them in a script like `scripts/env.sh`). Make sure the private key path points to an existing file; `~` is not expanded inside dbt, so use the absolute path or `realpath` output:
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
   export DBT_PROFILES_DIR="$(pwd)/dbt_pipeline"
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

   Alternatively you can stay in the repo root and prefix each command with `dbt --project-dir dbt_pipeline`; just keep the flag on the same line (no stray newlines) so dbt parses the argument correctly.

## Hosted dbt Docs

- Live DEV docs (GitHub Pages):  
  [https://sahilbhange.github.io/snowflake-dbt-stack/dbt_docs/#!/overview](https://sahilbhange.github.io/snowflake-dbt-stack/dbt_docs/#!/overview)

To regenerate and publish docs:

1. From `dbt_pipeline/`, run:
   ```bash
   dbt docs generate --target dev
   ```
2. From the repo root, refresh the static site:
   ```bash
   rm -rf docs/dbt_docs/*
   cp -R dbt_pipeline/target/* docs/dbt_docs/
   ```
3. Commit and push:
   ```bash
   git add docs/dbt_docs
   git commit -m "Refresh dbt docs (dev)"
   git push
   ```

Docs are served from the `/docs` folder on the `main` branch via GitHub Pages.

## CI/CD Overview

This repo includes a GitHub Actions workflow for dbt validation and dev testing:

- **Workflow file:** `.github/workflows/dbt-ci.yml`
- **Triggers:** Pull requests targeting `dev` or `main`.
- **Jobs:**
  - `validate_dev` – `dbt deps` + `dbt parse --target dev` + metadata checks + light SQLFluff on core/marts (no Snowflake compute).
  - `dev_tests` – `dbt compile --target dev` and `dbt test --target dev` against the Snowflake **DEV** database using secrets.
  - `validate_prod` – `dbt deps` + `dbt parse --target prod` + metadata checks (no Snowflake compute), runs only after `dev_tests` succeeds on PRs into `main`.
- **Orchestration:** All real PROD runs (`dbt run`, `dbt build`, `dbt test`) are handled by Snowflake Workspace jobs, not by GitHub.

For a full walkthrough of this CI/CD setup (secrets, job flow, and local equivalents), see `dbt_learning/dbt_cicd_github_actions.md`.

## Project Layout
- `scripts/` – raw SQL helpers (initial roles, manual ingest, workflow notes)
- `dbt_pipeline/` – dbt project: macros, models, snapshots, seeds, packages
- `plan.md` – living plan for the end-to-end state-of-the-art build
