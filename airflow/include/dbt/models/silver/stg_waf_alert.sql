{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key='event_id',
    on_schema_change='append_new_columns',
    partitioned_by=['day(event_ts)']
) }}

with source as (
    select * from {{ source('bronze', 'waf_alert') }}
    {{ incremental_window() }}
),

deduped as (
    select *,
        row_number() over (partition by event_id order by _ingest_ts desc, _kafka_offset desc) as rn
    from source
)

select
    event_id,
    device_id,
    tenant_id,
    product_version,
    cast(event_ts as timestamp(6)) as event_ts,
    schema_version,
    rule_id,
    severity,
    action,
    attack_category,
    request_id,
    cast(json_extract_scalar(details, '$.score') as integer) as match_score,
    json_extract_scalar(details, '$.matched_field') as matched_field,
    date_diff('minute', event_ts, _ingest_ts) > {{ var('late_after_minutes') }} as is_late,
    _kafka_partition,
    _kafka_offset,
    cast(_ingest_ts as timestamp(6)) as _ingest_ts
from deduped
where rn = 1


