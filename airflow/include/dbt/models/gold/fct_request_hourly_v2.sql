{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key='device_hour_key',
    update_condition='src._source_max_ingest_ts > target._source_max_ingest_ts',
    on_schema_change='fail',
    partitioned_by=['day(event_hour)']
) }}

with requests as (
    select * from {{ ref('stg_request_log') }}
    {{ changed_hours_filter(ref('stg_request_log')) }}
)

select
    r.device_id || '|' || cast(date_trunc('hour', r.event_ts) as varchar) as device_hour_key,
    r.device_id,
    date_trunc('hour', r.event_ts) as event_hour,
    count(*) as request_count,
    count_if(c.status_class = '4xx') as client_error_count,
    count_if(c.status_class = '5xx') as server_error_count,
    cast(count_if(c.is_error) as double) / count(*) as error_rate,
    approx_percentile(r.latency_ms, 0.5) as p50_latency_ms,
    approx_percentile(r.latency_ms, 0.95) as p95_latency_ms,
    cast(qdigest_agg(r.latency_ms) as varbinary) as latency_digest,
    sum(r.bytes_out) as bytes_out,
    count_if(r.is_late) as late_count,
    max(r._ingest_ts) as _source_max_ingest_ts
from requests r
left join {{ ref('http_status_codes') }} c on r.status_code = c.status_code
group by 1, 2, 3