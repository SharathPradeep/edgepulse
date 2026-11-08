{% macro changed_hours_filter(silver_model) %}
  {% if is_incremental() %}
  where date_trunc('hour', event_ts) in (
      select distinct date_trunc('hour', event_ts)
      from {{ silver_model }}
      where _ingest_ts > (select max(_source_max_ingest_ts) from {{ this }})
                         - interval '{{ var("lookback_hours") }}' hour
  )
  {% endif %}
{% endmacro %}