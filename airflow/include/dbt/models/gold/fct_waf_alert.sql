{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key='event_id',
    update_condition='src._ingest_ts > target._ingest_ts',
    on_schema_change='append_new_columns',
    partitioned_by=['day(event_ts)']
) }}

with alerts as (
    select * from {{ ref('stg_waf_alert') }}
    {{ incremental_window() }}
)

select
    a.event_id,
    a.event_ts,
    date_trunc('hour', a.event_ts) as alert_hour,
    a.device_id,
    a.rule_id,
    coalesce(r.attack_category, a.attack_category) as attack_category,
    a.severity,
    a.action,
    a.match_score,
    a.matched_field,
    a.is_late,
    a._ingest_ts
from alerts a
left join {{ ref('dim_waf_rule') }} r on a.rule_id = r.rule_id