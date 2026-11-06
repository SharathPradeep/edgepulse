# ADR 004: dbt Core on Athena with Iceberg tables

## Status
Accepted

## Context
Silver and gold need versioned, tested SQL transformations on top of the Iceberg bronze tables in Glue. The team should be able to review every change as code.

## Decision
- dbt Core 1.12 with dbt-athena 1.11, workgroup `edgepulse`, AWS profile `edgepulse`.
- Every dbt table, seeds included, is Iceberg (`+table_type: iceberg`) so the whole lake has snapshots, time travel and the same format.
- Table data lives under `s3://edgepulse-lake-sharath-0001/<schema>/<table>/` (`s3_data_naming: schema_table`).
- A `generate_schema_name` override writes to `ep_silver` and `ep_gold` exactly, without the dbt default prefix.
- Reference data (devices, WAF rules, HTTP status codes) is managed as seeds in Git.
- Bronze freshness is checked with `dbt source freshness` (warn at 15 min, error at 60 min), replacing the SLA feature removed in Airflow 3.

## Consequences
- The Athena workgroup must not override client-side settings, otherwise dbt-athena ignores `s3_data_dir` and writes tables under `athena-results/tables/<uuid>`. Found and fixed during setup.
- Seeds do not inherit model config, so the table type is set for them separately.
- Rebuilding a seed needs `dbt seed --full-refresh`.