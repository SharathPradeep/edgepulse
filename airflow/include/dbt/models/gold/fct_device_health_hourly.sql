{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key='device_hour_key',
    update_condition='src._source_max_ingest_ts > target._source_max_ingest_ts',
    on_schema_change='append_new_columns',
    partitioned_by=['day(event_hour)']
) }}

with metrics as (
    select * from {{ ref('stg_device_metric') }}
    {{ changed_hours_filter(ref('stg_device_metric')) }}
)

select
    device_id || '|' || cast(date_trunc('hour', event_ts) as varchar) as device_hour_key,
    device_id,
    date_trunc('hour', event_ts) as event_hour,
    count(*) as sample_count,
    avg(cpu_pct) as avg_cpu_pct,
    max(cpu_pct) as max_cpu_pct,
    avg(mem_pct) as avg_mem_pct,
    max(mem_pct) as max_mem_pct,
    avg(active_connections) as avg_connections,
    max(active_connections) as max_connections,
    avg(throughput_mbps) as avg_throughput_mbps,
    max(_ingest_ts) as _source_max_ingest_ts
from metrics
group by 1, 2, 3