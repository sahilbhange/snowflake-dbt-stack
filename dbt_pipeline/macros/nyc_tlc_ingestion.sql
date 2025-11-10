{% macro setup_nyc_tlc_stage_objects() %}
  {% set cmds = [
    "USE ROLE DBT_ETL",
    "USE WAREHOUSE DBT_WH_M",
    "USE DATABASE ANALYTICS",
    "CREATE SCHEMA IF NOT EXISTS ANALYTICS.UTIL",
    "USE SCHEMA UTIL",
    "CREATE FILE FORMAT IF NOT EXISTS ANALYTICS.UTIL.NYC_TLC_PARQUET_FF TYPE = PARQUET",
    "CREATE STAGE IF NOT EXISTS ANALYTICS.UTIL.NYC_TLC_STAGE FILE_FORMAT = ANALYTICS.UTIL.NYC_TLC_PARQUET_FF COMMENT = 'Internal stage for NYC TLC parquet uploads'"
  ] %}

  {% for stmt in cmds %}
    {{ log('Executing: ' ~ stmt, info=True) }}
    {% do run_query(stmt) %}
  {% endfor %}
{% endmacro %}


{% macro create_nyc_tlc_raw_tables() %}
  {% set datasets = [
    {'table': 'raw_tran_nyc_taxi_yellow', 'stage_folder': 'yellow'},
    {'table': 'raw_tran_nyc_taxi_green',  'stage_folder': 'green'},
    {'table': 'raw_tran_nyc_taxi_fhvhv',  'stage_folder': 'fhvhv'}
  ] %}

  {% do run_query('USE ROLE DBT_ETL') %}
  {% do run_query('USE WAREHOUSE DBT_WH_M') %}
  {% do run_query('USE DATABASE ANALYTICS') %}
  {% do run_query('USE SCHEMA RAW') %}

  {% for dataset in datasets %}
    {# Pre-check that INFER_SCHEMA finds at least one file; otherwise USING TEMPLATE will fail #}
    {% set stage_path = '@ANALYTICS.UTIL.NYC_TLC_STAGE/' ~ dataset.stage_folder %}
    {% set count_sql %}
      select count(*) as c
      from table(
        infer_schema(
          location => '{{ stage_path }}',
          file_format => 'ANALYTICS.UTIL.NYC_TLC_PARQUET_FF',
          ignore_case => true
        )
      )
    {% endset %}
    {% set count_res = run_query(count_sql) %}
    {% if execute %}
      {% set file_count = count_res.columns[0].values()[0] %}
    {% endif %}
    {% if file_count == 0 %}
      {{ log('No files found in stage path ' ~ stage_path ~ ' — cannot infer schema for ' ~ dataset.table, info=True) }}
      {% do exceptions.raise_compiler_error('Stage path ' ~ stage_path ~ ' is empty or not accessible. Upload at least one Parquet file (e.g., via SnowSQL PUT) then rerun the operation.') %}
    {% endif %}

    {% set stmt %}
      CREATE OR REPLACE TABLE ANALYTICS.RAW.{{ dataset.table }}
      USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(
          INFER_SCHEMA(
            LOCATION => '{{ stage_path }}',
            FILE_FORMAT => 'ANALYTICS.UTIL.NYC_TLC_PARQUET_FF',
            IGNORE_CASE => true
          )
        )
      )
    {% endset %}
    {{ log('Executing: ' ~ stmt, info=True) }}
    {% do run_query(stmt) %}
  {% endfor %}
{% endmacro %}


{% macro copy_nyc_tlc_from_stage() %}
  {% set datasets = [
    {'table': 'raw_tran_nyc_taxi_yellow', 'stage_folder': 'yellow'},
    {'table': 'raw_tran_nyc_taxi_green',  'stage_folder': 'green'},
    {'table': 'raw_tran_nyc_taxi_fhvhv',  'stage_folder': 'fhvhv'}
  ] %}

  {% do run_query('USE ROLE DBT_ETL') %}
  {% do run_query('USE WAREHOUSE DBT_WH_M') %}
  {% do run_query('USE DATABASE ANALYTICS') %}
  {% do run_query('USE SCHEMA RAW') %}

  {% for dataset in datasets %}
    {% set stmt %}
      COPY INTO ANALYTICS.RAW.{{ dataset.table }}
      FROM @ANALYTICS.UTIL.NYC_TLC_STAGE/{{ dataset.stage_folder }}
      FILE_FORMAT = (FORMAT_NAME = 'ANALYTICS.UTIL.NYC_TLC_PARQUET_FF')
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
      ON_ERROR = CONTINUE
    {% endset %}
    {{ log('Executing: ' ~ stmt, info=True) }}
    {% do run_query(stmt) %}
  {% endfor %}
{% endmacro %}


{% macro ingest_nyc_tlc_from_stage() %}
  {{ log('Ensuring NYC TLC stage objects exist', info=True) }}
  {{ setup_nyc_tlc_stage_objects() }}

  {{ log('Creating RAW layer tables via INFER_SCHEMA template', info=True) }}
  {{ create_nyc_tlc_raw_tables() }}

  {{ log('Copying staged parquet files into RAW tables', info=True) }}
  {{ copy_nyc_tlc_from_stage() }}

  {{ log('NYC TLC ingest complete', info=True) }}
{% endmacro %}
