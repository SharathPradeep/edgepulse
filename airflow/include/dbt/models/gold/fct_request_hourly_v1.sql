{{ config(materialized='view') }}

select
    device_hour_key, device_id, event_hour, request_count,
    client_error_count + server_error_count as error_count,
    error_rate, p50_latency_ms, p95_latency_ms, latency_digest,
    bytes_out, late_count, _source_max_ingest_ts
from {{ ref('fct_request_hourly', v=2) }}