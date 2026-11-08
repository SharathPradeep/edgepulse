{{ config(
    materialized='table',
    table_type='iceberg',
    s3_data_naming='schema_table_unique',
    partitioned_by=['day(event_hour)']
) }}

{% set w = "partition by device_id order by event_hour rows between 24 preceding and 1 preceding" %}
{% set p = "partition by device_id order by event_hour" %}

with bounds as (
    select min(event_hour) as min_hour, max(event_hour) as max_hour
    from {{ ref('fct_request_hourly', v=2) }}
    where event_hour >= (select max(event_hour) from {{ ref('fct_request_hourly', v=2) }})
                        - interval '{{ var("feature_days") }}' day
),

hours as (
    select h as event_hour
    from bounds
    cross join unnest(sequence(min_hour, max_hour, interval '1' hour)) as t(h)
),

spine as (
    select d.device_id, d.product_version as current_version, h.event_hour
    from {{ ref('dim_device') }} d
    cross join hours h
),

versions as (
    select device_id, date_trunc('hour', event_ts) as event_hour, max(product_version) as product_version
    from {{ ref('stg_request_log') }}
    where event_ts >= (select min_hour from bounds)
    group by 1, 2
),

alerts as (
    select device_id, alert_hour as event_hour,
           count(*) as alert_count,
           count_if(severity in ('high', 'critical')) as high_alert_count
    from {{ ref('fct_waf_alert') }}
    group by 1, 2
),

base as (
    select
        s.device_id,
        s.event_hour,
        coalesce(v.product_version, s.current_version) as product_version,
        coalesce(r.request_count, 0) as request_count,
        coalesce(r.error_rate, 0) as error_rate,
        r.p95_latency_ms,
        coalesce(r.late_count, 0) as late_count,
        coalesce(h.sample_count, 0) as metric_samples,
        h.avg_cpu_pct,
        h.max_cpu_pct,
        h.avg_mem_pct,
        coalesce(a.alert_count, 0) as alert_count,
        coalesce(a.high_alert_count, 0) as high_alert_count
    from spine s
    left join {{ ref('fct_request_hourly', v=2) }} r on r.device_id = s.device_id and r.event_hour = s.event_hour
    left join {{ ref('fct_device_health_hourly') }} h on h.device_id = s.device_id and h.event_hour = s.event_hour
    left join alerts a on a.device_id = s.device_id and a.event_hour = s.event_hour
    left join versions v on v.device_id = s.device_id and v.event_hour = s.event_hour
),

windowed as (
    select *,
        avg(request_count) over ({{ w }}) as req_mean,
        stddev_samp(request_count) over ({{ w }}) as req_std,
        avg(p95_latency_ms) over ({{ w }}) as p95_mean,
        stddev_samp(p95_latency_ms) over ({{ w }}) as p95_std,
        avg(error_rate) over ({{ w }}) as err_mean,
        stddev_samp(error_rate) over ({{ w }}) as err_std,
        avg(avg_cpu_pct) over ({{ w }}) as cpu_mean,
        stddev_samp(avg_cpu_pct) over ({{ w }}) as cpu_std,
        avg(case when metric_samples > 0 then 1.0 else 0.0 end) over ({{ w }}) as completeness_24h,
        count(*) over ({{ w }}) as history_hours,
        lag(request_count, 1) over ({{ p }}) as request_count_lag_1h,
        lag(request_count, 24) over ({{ p }}) as request_count_lag_24h,
        lag(p95_latency_ms, 1) over ({{ p }}) as p95_latency_lag_1h
    from base
)

select
    device_id || '|' || cast(event_hour as varchar) as device_hour_key,
    device_id,
    event_hour,
    product_version,
    request_count,
    error_rate,
    p95_latency_ms,
    avg_cpu_pct,
    max_cpu_pct,
    avg_mem_pct,
    alert_count,
    high_alert_count,
    request_count_lag_1h,
    request_count_lag_24h,
    p95_latency_lag_1h,
    coalesce((request_count - req_mean) / nullif(req_std, 0), 0) as request_count_z,
    coalesce((p95_latency_ms - p95_mean) / nullif(p95_std, 0), 0) as p95_latency_z,
    coalesce((error_rate - err_mean) / nullif(err_std, 0), 0) as error_rate_z,
    coalesce((avg_cpu_pct - cpu_mean) / nullif(cpu_std, 0), 0) as cpu_z,
    coalesce(completeness_24h, 0) as completeness_24h,
    case when request_count > 0 then cast(late_count as double) / request_count else 0 end as lateness_ratio,
    history_hours,
    history_hours = 24 as has_full_history,
    event_hour < date_trunc('hour', cast(current_timestamp at time zone 'UTC' as timestamp(6))) as is_closed_hour
from windowed